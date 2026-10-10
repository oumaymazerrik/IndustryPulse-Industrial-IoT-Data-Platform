import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    to_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)


# ============================================================
# 1. SPARK + ICEBERG
# ============================================================

spark = (
    SparkSession.builder
    .appName("IndustryPulse-Streaming-Iceberg")

    # Iceberg extensions
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # Iceberg Catalog
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog"
    )

    .config(
        "spark.sql.catalog.lakehouse.type",
        "hadoop"
    )

    # Iceberg Warehouse dans MinIO
    .config(
        "spark.sql.catalog.lakehouse.warehouse",
        "s3a://industrypulse/warehouse"
    )

    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. MINIO / S3A CONFIGURATION
# ============================================================

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

hadoop_conf.set(
    "fs.s3a.endpoint",
    os.getenv("MINIO_ENDPOINT", "http://minio:9000")
)

hadoop_conf.set(
    "fs.s3a.access.key",
    os.environ["MINIO_ROOT_USER"]
)

hadoop_conf.set(
    "fs.s3a.secret.key",
    os.environ["MINIO_ROOT_PASSWORD"]
)

hadoop_conf.set(
    "fs.s3a.path.style.access",
    "true"
)

hadoop_conf.set(
    "fs.s3a.connection.ssl.enabled",
    "false"
)

hadoop_conf.set(
    "fs.s3a.impl",
    "org.apache.hadoop.fs.s3a.S3AFileSystem"
)


# ============================================================
# 3. CREATE BRONZE NAMESPACE
# ============================================================

spark.sql("""
CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze
""")


# ============================================================
# 4. SCHEMA DES DONNEES IOT
# ============================================================

iot_schema = StructType([

    StructField(
        "machine_id",
        StringType(),
        False
    ),

    StructField(
        "temperature",
        DoubleType(),
        True
    ),

    StructField(
        "vibration",
        DoubleType(),
        True
    ),

    StructField(
        "pressure",
        DoubleType(),
        True
    ),

    StructField(
        "energy",
        DoubleType(),
        True
    ),

    StructField(
        "status",
        StringType(),
        True
    ),

    StructField(
        "timestamp",
        StringType(),
        True
    )
])


# ============================================================
# 5. READ KAFKA STREAM
# ============================================================

print("\n========================================")
print(" CONNECTING TO KAFKA")
print("========================================")

kafka_df = (
    spark.readStream
    .format("kafka")

    .option(
        "kafka.bootstrap.servers",
        "kafka:29092"
    )

    .option(
        "subscribe",
        "machine-sensors"
    )

    .option(
        "startingOffsets",
        "latest"
    )

    .load()
)


# ============================================================
# 6. CONVERT KAFKA BINARY DATA
# ============================================================

raw_kafka_df = kafka_df.select(

    col("key")
    .cast("string")
    .alias("kafka_key"),

    col("value")
    .cast("string")
    .alias("json_value"),

    col("partition")
    .alias("kafka_partition"),

    col("offset")
    .alias("kafka_offset"),

    col("timestamp")
    .alias("kafka_timestamp")

)


# ============================================================
# 7. PARSE JSON
# ============================================================

parsed_df = raw_kafka_df.select(

    from_json(
        col("json_value"),
        iot_schema
    ).alias("data"),

    col("kafka_partition"),

    col("kafka_offset"),

    col("kafka_timestamp")
)


# ============================================================
# 8. PREPARE BRONZE DATA
# ============================================================

bronze_df = parsed_df.select(

    col("data.machine_id")
    .alias("machine_id"),

    col("data.temperature")
    .alias("temperature"),

    col("data.vibration")
    .alias("vibration"),

    col("data.pressure")
    .alias("pressure"),

    col("data.energy")
    .alias("energy"),

    col("data.status")
    .alias("status"),

    to_timestamp(
        col("data.timestamp")
    ).alias("event_timestamp"),

    col("kafka_partition"),

    col("kafka_offset"),

    col("kafka_timestamp"),

    current_timestamp()
    .alias("ingestion_timestamp")
)


# ============================================================
# 9. DISPLAY STREAM SCHEMA
# ============================================================

print("\n========================================")
print(" BRONZE STREAM SCHEMA")
print("========================================")

bronze_df.printSchema()


# ============================================================
# 10. CREATE ICEBERG TABLE
# ============================================================

spark.sql("""
CREATE TABLE IF NOT EXISTS
lakehouse.bronze.machine_sensors_raw
(
    machine_id STRING,
    temperature DOUBLE,
    vibration DOUBLE,
    pressure DOUBLE,
    energy DOUBLE,
    status STRING,
    event_timestamp TIMESTAMP,
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP
)
USING iceberg
""")


print("\n========================================")
print(" ICEBERG BRONZE TABLE READY")
print("========================================")


# ============================================================
# 11. WRITE STREAM TO ICEBERG
# ============================================================

checkpoint_path = (
    "s3a://industrypulse/checkpoints/"
    "machine_sensors_bronze_v2"
)

query = (
    bronze_df.writeStream

    .format("iceberg")

    .outputMode("append")

    .option(
        "checkpointLocation",
        checkpoint_path
    )

    .trigger(
        processingTime="10 seconds"
    )

    .toTable(
        "lakehouse.bronze.machine_sensors_raw"
    )
)


print("\n========================================")
print(" STREAMING STARTED")
print(" Kafka -> Spark -> Iceberg Bronze")
print("========================================\n")


query.awaitTermination()