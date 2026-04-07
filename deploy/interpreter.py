import sys
import json
import os
import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col as p_col, lit, from_json, to_json, create_map, map_concat
from pyspark.sql.types import MapType, StringType

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KappaInterpreter")

def main():
    if len(sys.argv) < 2:
        logger.error("No JSON payload provided")
        return

    # 1. Parse Payload
    try:
        # Note: Go sends this as a JSON string
        payload = json.loads(sys.argv[1])
        topic_name = payload.get('table_pattern')        
        instructions = payload.get('instructions', [])
        logger.info(f"Starting stream for topic: {topic_name}")
    except Exception as e:
        logger.error(f"JSON Parse Failure: {e}")
        return

    # 2. Initialize Spark
    spark = SparkSession.builder \
        .appName(f"Nitric-analytics-{topic_name}") \
        .config("spark.sql.mapKeyDedupPolicy", "LAST_WIN") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    kafka_host = os.getenv("STREAM_SOURCE_HOST", "192.168.1.139")
    kafka_bootstrap = f"{kafka_host}:9092"

    # 3. Read Stream from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    # Convert binary Kafka value to string and map
    current_df = raw_stream.selectExpr("timestamp", "CAST(value AS STRING) as json_payload") \
        .withColumn("data_map", from_json(p_col("json_payload"), MapType(StringType(), StringType())))

    # 4. Processing Pipeline (Chained Instructions)
    output_mode = "append"
    target_topic = f"{topic_name}-results"
    
    for instr in instructions:
        # --- FILTER ---
        if "filter" in instr:
            c = instr["filter"]
            col_name = c.get('column')
            current_df = ensure_column(current_df, col_name)
            current_df = apply_filter_logic(current_df, col_name, c.get('operator'), c.get('value'))
            logger.info(f"Applied Filter: {col_name} {c.get('operator')} {c.get('value')}")

        # --- MAP TO STATE (Stateful Aggregation) ---
        elif "map_to_state" in instr:
            c = instr["map_to_state"]
            key = c.get('key_column')
            val_col = c.get('column')
            state_name = c.get('state_name')
            op = c.get('operation')

            current_df = ensure_column(current_df, key)
            current_df = ensure_column(current_df, val_col)

            # Perform the stateful aggregation
            # This collapses the DF into [key, state_name]
            if op == "INCREMENT":
                current_df = current_df.groupBy(p_col(key)).agg(F.sum(p_col(val_col).cast("double")).alias(state_name))
            elif op == "UPDATE":
                current_df = current_df.groupBy(p_col(key)).agg(F.last(p_col(val_col)).alias(state_name))
            
            # Aggregations REQUIRE 'update' mode to send changes to Kafka
            output_mode = "update"
            logger.info(f"Transitioned to Stateful: Grouped by {key}, State: {state_name}")

        # --- MAP (Stateless Transforms) ---
        elif "map" in instr:
            c = instr["map"]
            col_name = c.get('column')
            current_df = ensure_column(current_df, col_name)
            
            if c.get('operator') == "MULTIPLY":
                current_df = current_df.withColumn(col_name, p_col(col_name).cast("double") * float(c.get('value')))
            elif c.get('operator') == "UPPERCASE":
                current_df = current_df.withColumn(col_name, F.upper(p_col(col_name)))
            logger.info(f"Applied Map: {col_name} -> {c.get('operator')}")

        # --- SAVE ---
        elif "save" in instr:
            target_topic = instr["save"].get('target_table')
            logger.info(f"Set Target Topic: {target_topic}")

    # 5. Final Preparation for Kafka
    # If the DF has been aggregated, we can't use the original 'data_map'
    if "data_map" in current_df.columns:
        kafka_output_df = get_final_json_payload(current_df)
    else:
        # Aggregated DFs just send the resulting columns as JSON
        kafka_output_df = current_df.select(to_json(F.struct("*")).alias("value"))

    # 6. Start the Stream
    checkpoint_path = f"/tmp/checkpoints/{topic_name}_{target_topic}"
    
    query = kafka_output_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("topic", target_topic) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode(output_mode) \
        .start()

    query.awaitTermination()

# --- Helper Functions ---

def ensure_column(dataframe, column_name):
    """Ensures the column exists in the current schema, extracting from map if necessary."""
    if column_name in dataframe.columns:
        return dataframe
    if "data_map" in dataframe.columns:
        return dataframe.withColumn(column_name, p_col("data_map")[column_name])
    return dataframe

def apply_filter_logic(dataframe, column, operator, value):
    """Applies standard filtering logic."""
    c = p_col(column)
    # String comparisons
    if operator == "eq": return dataframe.filter(c == value)
    if operator == "neq": return dataframe.filter(c != value)
    
    # Numeric comparisons
    c_num = c.cast("double")
    v_num = float(value)
    if operator == "gt": return dataframe.filter(c_num > v_num)
    if operator == "lt": return dataframe.filter(c_num < v_num)
    return dataframe

def get_final_json_payload(dataframe):
    """Combines original data with any new calculated columns into a single JSON value."""
    # Identify calculated columns (not internal ones)
    internal = ["json_payload", "data_map", "timestamp"]
    calc_cols = [c for c in dataframe.columns if c not in internal]
    
    if not calc_cols:
        return dataframe.select(p_col("json_payload").alias("value"))

    # Build a map of the new columns
    map_args = []
    for c in calc_cols:
        map_args.append(lit(c))
        map_args.append(p_col(c).cast("string"))
    
    new_data_map = create_map(*map_args)
    # Merge original map with new data
    final_map = map_concat(p_col("data_map"), new_data_map)
    return dataframe.select(to_json(final_map).alias("value"))

if __name__ == "__main__":
    main()