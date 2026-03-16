import sys
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SparkInterpreter")

def main():
    if len(sys.argv) < 2:
        logger.error("No instruction JSON provided")
        return

    try:
        instruction = json.loads(sys.argv[1])
        table_pattern = instruction.get('table') 
        if not table_pattern:
            print("ERROR: No table pattern provided in instruction")
            sys.exit(1)
        commands = instruction.get('instructions', [])
    except Exception as e:
        logger.error(f"Failed to parse JSON: {e}")
        return

    spark = SparkSession.builder.appName(f"GoQuery_{table_pattern}").getOrCreate()

    try:
        # 1. Load data from Redis
        df = spark.read \
            .format("org.apache.spark.sql.redis") \
            .option("keys.pattern", table_pattern) \
            .option("infer.schema", "true") \
            .load()

        final_result = 0.0

        # 2. Iterate through the instructions list in order
        for cmd in commands:
            cmd_type = cmd.get('type')

            if cmd_type == "FILTER":
                column = cmd.get('column')
                operator = cmd.get('operator')
                value = cmd.get('value')

                logger.info(f"Applying Filter: {column} {operator} {value}")

                if operator == "eq":
                    df = df.filter(df[column] == value)
                elif operator == "neq": 
                    df = df.filter(df[column] != value)
                elif operator == "gt":
                    df = df.filter(df[column].cast("double") > float(value))
                elif operator == "lt":
                    df = df.filter(df[column].cast("double") < float(value))
                elif operator == "gte": 
                    df = df.filter(df[column].cast("double") >= float(value))
                elif operator == "lte": 
                    df = df.filter(df[column].cast("double") <= float(value))
                    
            elif cmd_type == "MAP":
                column = cmd.get('column')
                map_type = cmd.get('operator') # Changed variable name for clarity
                val = cmd.get('value')

                logger.info(f"Applying Map Operation: {map_type} on column {column}")

                if map_type == "MULTIPLY":
                    df = df.withColumn(column, col(column).cast("double") * float(val))
                elif map_type == "ADD":
                    df = df.withColumn(column, col(column).cast("double") + float(val))
                elif map_type == "UPPERCASE":
                    from pyspark.sql.functions import upper
                    df = df.withColumn(column, upper(col(column)))

            elif cmd_type == "SUM":
                column = cmd.get('column')
                logger.info(f"Applying Sum on: {column}")
                res = df.select(sum(col(column).cast("double"))).collect()[0][0]
                final_result = res if res is not None else 0.0

            elif cmd_type == "SAVE":
                target_table = cmd.get('value')
                logger.info(f"Saving filtered results to table: {target_table}")
                
                # Overwrite mode ensures the table is cleared before writing new data
                df.write \
                    .format("org.apache.spark.sql.redis") \
                    .option("table", target_table) \
                    .mode("overwrite") \
                    .save()
                
                # Return the count of records saved as the result
                final_result = float(df.count())

        # 3. Final Output for Go SDK
        print(f"RESULT_START:{final_result}:RESULT_END")

    except Exception as e:
        logger.error(f"Execution failed: {e}")
        print(f"RESULT_START:0.0:RESULT_END") 
    finally:
        spark.stop()

if __name__ == "__main__":
    main()