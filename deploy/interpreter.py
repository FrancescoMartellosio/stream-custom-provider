import sys
import json
import os
import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col as p_col, sum as p_sum, from_json, to_json, create_map, map_concat, lit
from pyspark.sql.types import MapType, StringType

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaInterpreter")

def main():
    if len(sys.argv) < 2:
        logger.error("No JSON payload provided")
        return

    try:
        payload = json.loads(sys.argv[1])
        topic_name = payload.get('table_pattern') 
        instructions = payload.get('instructions', [])
    except Exception as e:
        logger.error(f"JSON Parse Failure: {e}")
        return

    # Initialize Spark
    spark = SparkSession.builder \
        .appName(f"Nitric-Scanner-{topic_name}") \
        .config("spark.sql.mapKeyDedupPolicy", "LAST_WIN") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    kafka_host = os.getenv("STREAM_SOURCE_HOST", "192.168.1.86")
    kafka_bootstrap = f"{kafka_host}:9092"

    # Read from Kafka - Kept 'timestamp' for stateful chronological sorting
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON dynamically as a map of strings (No schema needed!)
    df = raw_stream.selectExpr("timestamp", "CAST(value AS STRING) as json_payload") \
        .withColumn("data_map", from_json(p_col("json_payload"), MapType(StringType(), StringType())))
    
    # State tracking for the final DAG construction
    has_sum = False
    sum_column = ""
    is_stateful = False
    state_key = ""
    sf_filter_col = ""
    sf_operator = ""
    sf_value = ""
    has_group_by = False
    group_by_cols = []
    target_topic = f"{topic_name}-results"

    # --- Instruction Processing Loop ---
    for instr in instructions:
        
        # 1. HANDLE FILTER
        if "filter" in instr:
            c = instr["filter"]
            df = ensure_column(df, c.get('column'))
            df = apply_filter_logic(df, c.get('column'), c.get('operator'), c.get('value'))
            logger.info(f"Added Filter: {c.get('column')} {c.get('operator')} {c.get('value')}")

        # 2. HANDLE STATEFUL FILTER (Deferred until Sink)
        elif "stateful_filter" in instr:
            c = instr["stateful_filter"]
            is_stateful = True
            state_key = c.get('key_column')
            sf_filter_col = c.get('column')
            sf_operator = c.get('operator')
            sf_value = c.get('value')
            
            df = ensure_column(df, sf_filter_col)
            df = ensure_column(df, state_key)
            logger.info(f"Marked Stateful-Filter: {sf_filter_col} {sf_operator} {sf_value} on Key: {state_key}")

        # 3. HANDLE MAP (Transformations)
        elif "map" in instr:
            c = instr["map"]
            col_name = c.get('column')
            df = ensure_column(df, col_name)
            
            logger.info(f"Added Map: {col_name} -> {c.get('operator')}")
            if c.get('operator') == "MULTIPLY":
                df = df.withColumn(col_name, p_col(col_name).cast("double") * float(c.get('value')))
            elif c.get('operator') == "ADD":
                df = df.withColumn(col_name, p_col(col_name).cast("double") + float(c.get('value')))
            elif c.get('operator') == "UPPERCASE":
                df = df.withColumn(col_name, F.upper(p_col(col_name)))

        # 4. HANDLE GROUP BY (Deferred until aggregation)
        elif "group_by" in instr:
            has_group_by = True
            group_by_cols = instr["group_by"].get("columns", [])
            for c_name in group_by_cols:
                df = ensure_column(df, c_name)
            logger.info(f"Marked GroupBy: {group_by_cols}")

        # 5. HANDLE SUM
        elif "sum" in instr: 
            has_sum = True
            sum_column = instr["sum"].get("column")
            df = ensure_column(df, sum_column)
            logger.info(f"Marked Sum on: {sum_column}")

        # 6. HANDLE SAVE DESTINATION
        elif "save" in instr:
            target_topic = instr["save"].get("target_table")
            logger.info(f"Targeting Topic: {target_topic}")

    # --- Sink Configuration ---
    checkpoint_path = f"/tmp/checkpoints/{topic_name}_{target_topic}"

    # Helper function to reconstruct the map with modified overrides
    def get_final_json_payload(dataframe):
        extracted_cols = [c for c in dataframe.columns if c not in ["json_payload", "data_map"]]
        
        if extracted_cols:
            map_args = []
            for c in extracted_cols:
                map_args.append(lit(c))
                map_args.append(p_col(c).cast("string"))
            
            override_map = create_map(*map_args)
            final_map = map_concat(p_col("data_map"), override_map)
            return dataframe.select(to_json(final_map).alias("value"))
        
        return dataframe.select(to_json(p_col("data_map")).alias("value"))


    # NEW: Stateful Filter Sink Handling
    if is_stateful:
        # 1. Grab every column we've accumulated (including map overrides) except key/timestamp
        agg_cols = [c for c in df.columns if c not in [state_key, "timestamp"]]
        
        # 2. Use F.max_by to ensure that ALL columns retain their latest state grouped by the key
        agg_exprs = [F.max_by(p_col(c), p_col("timestamp")).alias(c) for c in agg_cols]
        
        deduped_df = df.groupBy(state_key).agg(*agg_exprs)
        
        # 3. Apply the dynamic filter logic on the deduplicated materialized view
        filtered_df = apply_filter_logic(deduped_df, sf_filter_col, sf_operator, sf_value)
        
        # 4. Collapse extracted columns back into a JSON payload
        kafka_output_df = get_final_json_payload(filtered_df)

        # 5. Write using complete mode to pump all currently valid records whenever state updates
        query = kafka_output_df.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap) \
            .option("topic", target_topic) \
            .option("checkpointLocation", checkpoint_path) \
            .outputMode("complete") \
            .start()

    else:
        if has_sum:
            if has_group_by:
                output_df = df.groupBy(*group_by_cols).agg(p_sum(p_col(sum_column).cast("double")).alias("running_total"))
            else:
                output_df = df.select(p_sum(p_col(sum_column).cast("double")).alias("running_total"))
            
            kafka_output_df = output_df.selectExpr("CAST(running_total AS STRING) AS value")
            output_mode = "complete"
        else:
            if has_group_by:
                output_df = df.groupBy(*group_by_cols).count()
                kafka_output_df = output_df.select(to_json(p_col("*")).alias("value"))
                output_mode = "complete"
            else:
                kafka_output_df = get_final_json_payload(df)
                output_mode = "append"

        query = kafka_output_df.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap) \
            .option("topic", target_topic) \
            .option("checkpointLocation", checkpoint_path) \
            .outputMode(output_mode) \
            .start()

    query.awaitTermination()

# --- Helper Utilities ---

def ensure_column(dataframe, column_name):
    """Extracts column from the parsed data_map if not already present."""
    if column_name and column_name not in dataframe.columns:
        return dataframe.withColumn(column_name, p_col("data_map")[column_name])
    return dataframe

def apply_filter_logic(dataframe, column, operator, value):
    """Applies SQL-style filtering to the stream."""
    c = p_col(column)
    if operator == "eq": return dataframe.filter(c == value)
    if operator == "neq": return dataframe.filter(c != value)
    
    c_num = c.cast("double")
    v_num = float(value)
    if operator == "gt": return dataframe.filter(c_num > v_num)
    if operator == "lt": return dataframe.filter(c_num < v_num)
    if operator == "gte": return dataframe.filter(c_num >= v_num)
    if operator == "lte": return dataframe.filter(c_num <= v_num)
    
    logger.warning(f"Unknown operator {operator}, bypassing filter.")
    return dataframe

if __name__ == "__main__":
    main()