from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, TimestampType
import os

# ==============================================================================
# 1. ENVIRONMENT CONFIGURATION
# CRITICAL FIX: Set the version variables to match your environment's 4.0.1 installation.
# You must install PySpark 4.0.1 in your virtual environment for this to work.
# SPARK 4.x is typically built with Scala 2.13.
# ==============================================================================
KAFKA_VERSION_ID = "0-10"
SPARK_SCALA_VERSION = "2.13" # IMPORTANT: Spark 4.0 uses Scala 2.13
SPARK_VERSION = "4.0.1" # Targeting the version reported by your system

# ==============================================================================
# 2. SPARK SESSION SETUP
# CRITICAL CHANGE: Re-adding Hadoop/Azure JARs, and ensuring PySpark connects to Kafka correctly.
# ==============================================================================

# List of all required JAR packages
PACKAGES = [
    # Core Kafka Connector (Updated for Spark 4.0.1/Scala 2.13)
    f"org.apache.spark:spark-sql-kafka-{KAFKA_VERSION_ID}_{SPARK_SCALA_VERSION}:{SPARK_VERSION}",
    # PostgreSQL Driver
    "org.postgresql:postgresql:42.6.0",
    # Hadoop (needed for general Spark I/O and sometimes underlying connections)
    "org.apache.hadoop:hadoop-client-runtime:3.3.4",
    "org.apache.hadoop:hadoop-client-api:3.3.4",
]

spark = SparkSession.builder \
    .appName("KafkaToPostgres") \
    .config("spark.jars.packages", ",".join(PACKAGES)) \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.checkpointLocation", "file:///tmp/spark_finflow_checkpoint") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("="*50)
print(f"Spark Session initialized. Connecting to Kafka/Postgres on localhost.")
print("="*50)


# ==============================================================================
# 3. DATA SCHEMA AND STREAM READING
# ==============================================================================

schema = StructType() \
    .add("transaction_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("user_id", IntegerType()) \
    .add("merchant", StringType()) \
    .add("category", StringType()) \
    .add("amount", DoubleType()) \
    .add("currency", StringType()) \
    .add("location", StringType()) \
    .add("status", StringType())

# Read messages from Kafka Topic: transactions
# Using 'localhost:9092' for external connection to the Dockerized Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingoffsets", "latest") \
    .load()

# Parse Json messages
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")


# ==============================================================================
# 4. FOREACHBATCH WRITE FUNCTION
# Using 'localhost:5432' for the Postgres connection since Spark is running externally
# ==============================================================================

def write_to_postgres(batch_df, batch_id):
    if not batch_df.isEmpty():
        jdbc_url = "jdbc:postgresql://localhost:5432/finflow"
        
        # DEBUGGING: Show the data being written
        print(f"--- Batch {batch_id} ready for writing ({batch_df.count()} rows) ---")

        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "raw_transactions") \
                .option("user", "finflow") \
                .option("password", "finflow") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            print(f"--- Successfully wrote batch {batch_id} to PostgreSQL ---")
        except Exception as e:
            # This often happens if the 'raw_transactions' table does not exist.
            print(f"!!! Error writing batch {batch_id} to PostgreSQL (Is 'raw_transactions' table created?): {e} !!!")


# ==============================================================================
# 5. START STREAMING QUERY
# ==============================================================================

query = parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .start()

print(f"Structured Stream started. Connects to Kafka on: localhost:9092")

# Wait for the termination signal
query.awaitTermination()
