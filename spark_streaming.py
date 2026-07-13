import os
import sys
import sqlite3
import threading

# Dynamic JAVA_HOME setup for portable JDK
script_dir = os.path.dirname(os.path.abspath(__file__))
local_jdk_path = os.path.join(script_dir, "jdk")
if not os.environ.get("JAVA_HOME") and os.path.exists(local_jdk_path):
    subdirs = [os.path.join(local_jdk_path, d) for d in os.listdir(local_jdk_path) if os.path.isdir(os.path.join(local_jdk_path, d))]
    if subdirs:
        os.environ["JAVA_HOME"] = subdirs[0]
        os.environ["PATH"] = os.path.join(subdirs[0], "bin") + os.path.pathsep + os.environ.get("PATH", "")
        print(f"Dynamically set JAVA_HOME to: {os.environ['JAVA_HOME']}")

# Dynamic HADOOP_HOME setup for portable winutils
local_hadoop_path = os.path.join(script_dir, "hadoop")
if not os.environ.get("HADOOP_HOME") and os.path.exists(local_hadoop_path):
    os.environ["HADOOP_HOME"] = local_hadoop_path
    os.environ["PATH"] = os.path.join(local_hadoop_path, "bin") + os.path.pathsep + os.environ.get("PATH", "")
    print(f"Dynamically set HADOOP_HOME to: {os.environ['HADOOP_HOME']}")


from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, sum, avg, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

DB_PATH = os.path.join(script_dir, "smartlogix.db")
db_lock = threading.Lock()

def execute_db_write(write_fn):
    """Helper to write to SQLite with a thread lock to avoid database locked errors."""
    with db_lock:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        try:
            write_fn(conn)
        except Exception as e:
            print(f"Database write error: {e}", file=sys.stderr)
        finally:
            conn.close()

# Spark session setup with Kafka SQL Package
spark = SparkSession.builder \
    .appName("SmartLogix-Streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define Shipment Event JSON schema
schema = StructType([
    StructField("shipment_id", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("weight", DoubleType(), True),
    StructField("priority", StringType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# Read stream from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "shipment-events") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON values
parsed_stream = kafka_stream \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), schema).alias("data")) \
    .select("data.*")

# 1. Raw Shipments Sink
def write_raw(df, epoch_id):
    pandas_df = df.toPandas()
    if not pandas_df.empty:
        def write_fn(conn):
            pandas_df.to_sql("shipments", conn, if_exists="append", index=False)
        execute_db_write(write_fn)

q_raw = parsed_stream.writeStream \
    .trigger(processingTime="1 second") \
    .foreachBatch(write_raw) \
    .start()

# 2. Overall KPIs
# Since we need running totals, Spark structured streaming stateful aggregation in Complete mode tracks this.
kpi_stream = parsed_stream.groupBy().agg(
    count("shipment_id").alias("total_shipments"),
    sum("revenue").alias("total_revenue"),
    avg("weight").alias("avg_weight")
)

def write_kpis(df, epoch_id):
    pandas_df = df.toPandas()
    if not pandas_df.empty:
        def write_fn(conn):
            pandas_df.to_sql("kpis", conn, if_exists="replace", index=False)
        execute_db_write(write_fn)

q_kpis = kpi_stream.writeStream \
    .outputMode("complete") \
    .trigger(processingTime="2 seconds") \
    .foreachBatch(write_kpis) \
    .start()

# 3. City Metrics
city_stream = parsed_stream.groupBy("destination").agg(
    count("shipment_id").alias("shipment_count"),
    sum("revenue").alias("revenue"),
    avg("weight").alias("avg_weight")
)

def write_city_metrics(df, epoch_id):
    pandas_df = df.toPandas()
    if not pandas_df.empty:
        def write_fn(conn):
            pandas_df.to_sql("city_metrics", conn, if_exists="replace", index=False)
        execute_db_write(write_fn)

q_city = city_stream.writeStream \
    .outputMode("complete") \
    .trigger(processingTime="2 seconds") \
    .foreachBatch(write_city_metrics) \
    .start()

# 4. Status Metrics (Delivered vs In Transit vs others)
status_stream = parsed_stream.groupBy("status").count()

def write_status_metrics(df, epoch_id):
    pandas_df = df.toPandas()
    if not pandas_df.empty:
        def write_fn(conn):
            pandas_df.to_sql("status_metrics", conn, if_exists="replace", index=False)
        execute_db_write(write_fn)

q_status = status_stream.writeStream \
    .outputMode("complete") \
    .trigger(processingTime="2 seconds") \
    .foreachBatch(write_status_metrics) \
    .start()

# 5. Vehicle Utilization (Unique vehicles)
# Spark does not support countDistinct on streaming DF. We group by vehicle_id and count rows in Python.
vehicle_stream = parsed_stream.groupBy("vehicle_id").count()

def write_vehicle_metrics(df, epoch_id):
    pandas_df = df.toPandas()
    if not pandas_df.empty:
        unique_count = len(pandas_df)
        def write_fn(conn):
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS vehicle_metrics (unique_vehicles INTEGER)")
            cursor.execute("DELETE FROM vehicle_metrics")
            cursor.execute("INSERT INTO vehicle_metrics VALUES (?)", (unique_count,))
            conn.commit()
        execute_db_write(write_fn)

q_vehicles = vehicle_stream.writeStream \
    .outputMode("complete") \
    .trigger(processingTime="2 seconds") \
    .foreachBatch(write_vehicle_metrics) \
    .start()

# 6. EOD Challenge: Filter High Priority and compute city-wise counts and avg weights
high_priority_stream = parsed_stream \
    .filter(col("priority") == "High") \
    .groupBy("destination") \
    .agg(
        count("shipment_id").alias("shipment_count"),
        avg("weight").alias("avg_weight")
    )

def write_high_priority_metrics(df, epoch_id):
    pandas_df = df.toPandas()
    if not pandas_df.empty:
        def write_fn(conn):
            pandas_df.to_sql("high_priority_metrics", conn, if_exists="replace", index=False)
        execute_db_write(write_fn)

q_high_priority = high_priority_stream.writeStream \
    .outputMode("complete") \
    .trigger(processingTime="2 seconds") \
    .foreachBatch(write_high_priority_metrics) \
    .start()

# 7. Windowed Analytics (10 seconds Tumbling Window with 10 seconds Watermark)
windowed_stream = parsed_stream \
    .withColumn("timestamp_parsed", col("timestamp").cast("timestamp")) \
    .withWatermark("timestamp_parsed", "10 seconds") \
    .groupBy(
        window(col("timestamp_parsed"), "10 seconds"),
        col("destination")
    ) \
    .agg(
        count("shipment_id").alias("shipment_count"),
        avg("weight").alias("avg_weight")
    )

def write_windowed_metrics(df, epoch_id):
    flat_df = df.select(
        col("window.start").cast("string").alias("window_start"),
        col("window.end").cast("string").alias("window_end"),
        col("destination"),
        col("shipment_count"),
        col("avg_weight")
    )
    pandas_df = flat_df.toPandas()
    if not pandas_df.empty:
        def write_fn(conn):
            pandas_df.to_sql("windowed_metrics", conn, if_exists="replace", index=False)
        execute_db_write(write_fn)

q_windowed = windowed_stream.writeStream \
    .outputMode("complete") \
    .trigger(processingTime="5 seconds") \
    .foreachBatch(write_windowed_metrics) \
    .start()

print("All Spark Streaming queries started. Listening for Kafka events...")

# Await termination
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Stopping Spark Streaming application...")
