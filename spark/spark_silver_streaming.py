from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    current_timestamp
)


# ============================================================
# 1. SPARK + ICEBERG
# ============================================================

spark = (
    SparkSession.builder
    .appName("IndustryPulse-Silver-Streaming")

    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    .config(
        "spark.sql.catalog.industrypulse",
        "org.apache.iceberg.spark.SparkCatalog"
    )

    .config(
        "spark.sql.catalog.industrypulse.type",
        "hadoop"
    )

    .config(
        "spark.sql.catalog.industrypulse.warehouse",
        "s3a://industrypulse/warehouse"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. MINIO / S3A
# ============================================================

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

hadoop_conf.set(
    "fs.s3a.endpoint",
    "http://minio:9000"
)

hadoop_conf.set(
    "fs.s3a.access.key",
    "admin"
)

hadoop_conf.set(
    "fs.s3a.secret.key",
    "industrypulse123"
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
# 3. SILVER NAMESPACE
# ============================================================

spark.sql("""
CREATE NAMESPACE IF NOT EXISTS industrypulse.silver
""")


# ============================================================
# 4. CREATE SILVER ICEBERG TABLE
# ============================================================

spark.sql("""
CREATE TABLE IF NOT EXISTS
industrypulse.silver.machine_sensors_clean
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

    ingestion_timestamp TIMESTAMP,

    temperature_alert BOOLEAN,
    vibration_alert BOOLEAN,
    sensor_alert BOOLEAN,

    data_quality_status STRING,

    processing_timestamp TIMESTAMP
)
USING iceberg
""")


print("\n========================================")
print(" SILVER ICEBERG TABLE READY")
print("========================================")


# ============================================================
# 5. READ BRONZE AS STREAM
# ============================================================

bronze_df = (
    spark.readStream
    .format("iceberg")
    .load(
        "industrypulse.bronze.machine_sensors_raw"
    )
)


print("\n========================================")
print(" BRONZE STREAM SCHEMA")
print("========================================")

bronze_df.printSchema()


# ============================================================
# 6. DATA QUALITY
# ============================================================

validated_df = bronze_df.withColumn(

    "data_quality_status",

    when(
        col("machine_id").isNull(),
        "INVALID_MACHINE_ID"
    )

    .when(
        col("event_timestamp").isNull(),
        "INVALID_TIMESTAMP"
    )

    .when(
        col("temperature").isNull(),
        "INVALID_TEMPERATURE"
    )

    .when(
        (col("temperature") < 0)
        | (col("temperature") > 150),
        "INVALID_TEMPERATURE"
    )

    .when(
        col("vibration").isNull(),
        "INVALID_VIBRATION"
    )

    .when(
        (col("vibration") < 0)
        | (col("vibration") > 5),
        "INVALID_VIBRATION"
    )

    .when(
        col("pressure").isNull(),
        "INVALID_PRESSURE"
    )

    .when(
        (col("pressure") < 0)
        | (col("pressure") > 20),
        "INVALID_PRESSURE"
    )

    .when(
        col("energy").isNull(),
        "INVALID_ENERGY"
    )

    .when(
        col("energy") < 0,
        "INVALID_ENERGY"
    )

    .otherwise(
        "VALID"
    )
)


# ============================================================
# 7. KEEP VALID DATA
# ============================================================

clean_df = validated_df.filter(
    col("data_quality_status") == "VALID"
)


# ============================================================
# 8. ADD ALERT FEATURES
# ============================================================

silver_df = (

    clean_df

    # --------------------------------------------------------
    # Temperature alert
    # --------------------------------------------------------

    .withColumn(
        "temperature_alert",

        when(
            col("temperature") > 85,
            True
        ).otherwise(False)
    )


    # --------------------------------------------------------
    # Vibration alert
    # --------------------------------------------------------

    .withColumn(
        "vibration_alert",

        when(
            col("vibration") > 1.0,
            True
        ).otherwise(False)
    )


    # --------------------------------------------------------
    # Global sensor alert
    # --------------------------------------------------------

    .withColumn(
        "sensor_alert",

        when(
            (col("temperature") > 85)
            | (col("vibration") > 1.0),
            True
        ).otherwise(False)
    )


    # --------------------------------------------------------
    # Processing timestamp
    # --------------------------------------------------------

    .withColumn(
        "processing_timestamp",
        current_timestamp()
    )
)


# ============================================================
# 9. SELECT FINAL SILVER SCHEMA
# ============================================================

silver_df = silver_df.select(

    "machine_id",

    "temperature",
    "vibration",
    "pressure",
    "energy",

    "status",

    "event_timestamp",

    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",

    "ingestion_timestamp",

    "temperature_alert",
    "vibration_alert",
    "sensor_alert",

    "data_quality_status",

    "processing_timestamp"
)


print("\n========================================")
print(" SILVER STREAM SCHEMA")
print("========================================")

silver_df.printSchema()


# ============================================================
# 10. CHECKPOINT
# ============================================================

checkpoint_path = (
    "s3a://industrypulse/checkpoints/"
    "machine_sensors_silver"
)


# ============================================================
# 11. WRITE SILVER STREAM
# ============================================================

query = (

    silver_df.writeStream

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
        "industrypulse.silver.machine_sensors_clean"
    )
)


print("\n========================================")
print(" SILVER STREAMING STARTED")
print(" Bronze -> Spark -> Silver")
print("========================================\n")


query.awaitTermination()