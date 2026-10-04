from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    window,
    avg,
    min,
    max,
    count,
    sum as spark_sum,
    round as spark_round,
    current_timestamp
)


# ============================================================
# 1. SPARK + ICEBERG
# ============================================================

spark = (
    SparkSession.builder
    .appName("IndustryPulse-Gold-Streaming")

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
# 3. GOLD NAMESPACE
# ============================================================

spark.sql("""
CREATE NAMESPACE IF NOT EXISTS industrypulse.gold
""")


# ============================================================
# 4. GOLD TABLE
# ============================================================

spark.sql("""
CREATE TABLE IF NOT EXISTS
industrypulse.gold.machine_kpi
(
    machine_id STRING,

    window_start TIMESTAMP,
    window_end TIMESTAMP,

    total_measurements BIGINT,

    avg_temperature DOUBLE,
    min_temperature DOUBLE,
    max_temperature DOUBLE,

    avg_vibration DOUBLE,
    max_vibration DOUBLE,

    avg_pressure DOUBLE,

    avg_energy DOUBLE,

    total_alerts BIGINT,

    alert_rate DOUBLE,

    gold_processing_timestamp TIMESTAMP
)
USING iceberg
""")


print("\n========================================")
print(" GOLD ICEBERG TABLE READY")
print("========================================")


# ============================================================
# 5. READ SILVER STREAM
# ============================================================

silver_df = (
    spark.readStream
    .format("iceberg")
    .load(
        "industrypulse.silver.machine_sensors_clean"
    )
)


print("\n========================================")
print(" SILVER STREAM LOADED")
print("========================================")

silver_df.printSchema()


# ============================================================
# 6. WATERMARK
# ============================================================

stream_df = (
    silver_df
    .withWatermark(
        "event_timestamp",
        "1 minute"
    )
)


# ============================================================
# 7. KPI AGGREGATION
# ============================================================

gold_df = (
    stream_df

    .groupBy(
        window(
            col("event_timestamp"),
            "1 minute"
        ),
        col("machine_id")
    )

    .agg(

        count("*")
        .alias("total_measurements"),

        spark_round(
            avg("temperature"),
            2
        ).alias("avg_temperature"),

        spark_round(
            min("temperature"),
            2
        ).alias("min_temperature"),

        spark_round(
            max("temperature"),
            2
        ).alias("max_temperature"),

        spark_round(
            avg("vibration"),
            3
        ).alias("avg_vibration"),

        spark_round(
            max("vibration"),
            3
        ).alias("max_vibration"),

        spark_round(
            avg("pressure"),
            2
        ).alias("avg_pressure"),

        spark_round(
            avg("energy"),
            2
        ).alias("avg_energy"),

        spark_sum(
            col("sensor_alert").cast("long")
        ).alias("total_alerts")
    )
)


# ============================================================
# 8. FLATTEN WINDOW + ALERT RATE
# ============================================================

gold_df = (
    gold_df

    .withColumn(
        "window_start",
        col("window.start")
    )

    .withColumn(
        "window_end",
        col("window.end")
    )

    .withColumn(
        "alert_rate",
        spark_round(
            (
                col("total_alerts")
                / col("total_measurements")
            ) * 100,
            2
        )
    )

    .withColumn(
        "gold_processing_timestamp",
        current_timestamp()
    )

    .select(
        "machine_id",

        "window_start",
        "window_end",

        "total_measurements",

        "avg_temperature",
        "min_temperature",
        "max_temperature",

        "avg_vibration",
        "max_vibration",

        "avg_pressure",

        "avg_energy",

        "total_alerts",
        "alert_rate",

        "gold_processing_timestamp"
    )
)


print("\n========================================")
print(" GOLD STREAM SCHEMA")
print("========================================")

gold_df.printSchema()


# ============================================================
# 9. CHECKPOINT
# ============================================================

checkpoint_path = (
    "s3a://industrypulse/checkpoints/"
    "machine_kpi_gold"
)


# ============================================================
# 10. WRITE GOLD
# ============================================================

query = (
    gold_df.writeStream

    .format("iceberg")

    .outputMode("append")

    .option(
        "checkpointLocation",
        checkpoint_path
    )

    .trigger(
        processingTime="20 seconds"
    )

    .toTable(
        "industrypulse.gold.machine_kpi"
    )
)


print("\n========================================")
print(" GOLD STREAMING STARTED")
print(" Silver -> KPI -> Gold")
print("========================================\n")


query.awaitTermination()