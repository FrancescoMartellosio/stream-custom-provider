import sys
import json
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, upper, sum as spark_sum, struct, to_json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaInterpreter")

def main():
    # 1. Parse Input
    if len(sys.argv) < 2:
        return

    try:
        instructions = json.loads(sys.argv[1])
        topic_name = instructions.get('topic_name')
        commands = instructions.get('instructions', [])
    except Exception as e:
        logger.error(f"JSON Parse Failure: {e}")
        return

    # 2. Setup Spark
    spark = SparkSession.builder.appName(f"Nitric-Scanner-{topic_name}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    kafka_host = os.getenv("STREAM_SOURCE_HOST", "192.168.1.23")
    kafka_bootstrap = f"{kafka_host}:9092"

    # 3. Read Stream
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    # We keep 'json_payload' but we will build real columns alongside it
    df = raw_stream.selectExpr("CAST(value AS STRING) as json_payload")
    
    # 4. Concatenated Instruction Loop
    has_sum = False
    sum_column = ""

    for cmd in commands:
        cmd_type = cmd.get("type", "").upper()
        column = cmd.get("column")
        operator = cmd.get("operator")
        value = cmd.get("value")

        # CRITICAL: If the column isn't in our DF yet, extract it from JSON.
        # If it IS already there (from a previous MAP), we use the existing one.
        if column and column not in df.columns:
            df = df.withColumn(column, get_json_object(col("json_payload"), f"$.{column}"))

        if cmd_type == "FILTER":
            logger.info(f"Chain-Filter: {column} {operator} {value}")
            if operator == "eq":
                df = df.filter(col(column) == value)
            elif operator == "neq":
                df = df.filter(col(column) != value)
            elif operator == "gt":
                df = df.filter(col(column).cast("double") > float(value))
            elif operator == "lt":
                df = df.filter(col(column).cast("double") < float(value))
            elif operator == "gte":
                df = df.filter(col(column).cast("double") >= float(value))
            elif operator == "lte":
                df = df.filter(col(column).cast("double") <= float(value))

        elif cmd_type == "MAP":
            logger.info(f"Chain-Map: {column} -> {operator}")
            if operator == "MULTIPLY":
                df = df.withColumn(column, col(column).cast("double") * float(value))
            elif operator == "ADD":
                df = df.withColumn(column, col(column).cast("double") + float(value))
            elif operator == "UPPERCASE":
                df = df.withColumn(column, upper(col(column)))

        elif cmd_type == "SUM": 
            has_sum = True
            sum_column = column

    # 5. Sink Configuration
    if has_sum:
        # Aggregate the column (after all previous MAPs/FILTERs)
        output_df = df.select(spark_sum(col(sum_column).cast("double")).alias("running_total"))
        kafka_output_df = output_df.selectExpr("CAST(running_total AS STRING) AS value")
        output_mode = "complete"
    else:
        # Final output includes all transformed columns
        # We drop the raw json_payload so the output is clean JSON of the results
        final_df = df.drop("json_payload")
        kafka_output_df = final_df.select(to_json(struct("*")).alias("value"))
        output_mode = "append"

    # 6. Start Stream
    target_topic = next((c.get('value') for c in commands if c.get('type').upper() == "SAVE"), f"{topic_name}-results")
    checkpoint_path = f"/tmp/checkpoints/{topic_name}_{target_topic}"

    query = kafka_output_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("topic", target_topic) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode(output_mode) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()