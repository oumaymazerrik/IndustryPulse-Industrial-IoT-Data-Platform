
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    current_timestamp,
    lit,
    isnan
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
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog"
    )

    .config(
        "spark.sql.catalog.lakehouse.type",
        "hadoop"
    )

    .config(
        "spark.sql.catalog.lakehouse.warehouse",
        "s3a://industrypulse/warehouse"
    )

    .config(
        "spark.hadoop.fs.s3a.endpoint",
        os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    )

    .config(
        "spark.hadoop.fs.s3a.access.key",
        os.environ["MINIO_ROOT_USER"]
    )

    .config(
        "spark.hadoop.fs.s3a.secret.key",
        os.environ["MINIO_ROOT_PASSWORD"]
    )

    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. MINIO / S3A
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

hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


# ============================================================
# 3. SILVER NAMESPACE
# ============================================================

spark.sql("""
CREATE NAMESPACE IF NOT EXISTS lakehouse.silver
""")


# ============================================================
# 4. CREATE SILVER ICEBERG TABLE
# ============================================================

table_name = "lakehouse.silver.machine_sensors_clean"

spark.sql("""
CREATE TABLE IF NOT EXISTS
lakehouse.silver.machine_sensors_clean
(
    machine_id STRING,

    temperature DOUBLE,
    vibration DOUBLE,
    pressure DOUBLE,
    energy DOUBLE,

    air_temperature DOUBLE,
    process_temperature DOUBLE,
    rotational_speed DOUBLE,
    torque DOUBLE,
    tool_wear DOUBLE,

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
    ml_data_quality_status STRING,

    processing_timestamp TIMESTAMP
)
USING iceberg
""")


# ============================================================
# 5. EVOLVE EXISTING SILVER TABLE
# ============================================================

existing_columns = {
    field.name
    for field in spark.table(table_name).schema.fields
}

new_columns = {
    "air_temperature": "DOUBLE",
    "process_temperature": "DOUBLE",
    "rotational_speed": "DOUBLE",
    "torque": "DOUBLE",
    "tool_wear": "DOUBLE",
    "ml_data_quality_status": "STRING"
}

for column_name, column_type in new_columns.items():

    if column_name not in existing_columns:

        spark.sql(
            f"ALTER TABLE {table_name} "
            f"ADD COLUMN {column_name} {column_type}"
        )

        print(
            f"[OK] Colonne ajoutee : {column_name}",
            flush=True
        )

print("[OK] Table Iceberg Silver prete", flush=True)


# ============================================================
# 6. READ BRONZE AS STREAM
# ============================================================

bronze_df = (
    spark.readStream
    .format("iceberg")
    .load("lakehouse.bronze.machine_sensors_raw")
)

print("\n========================================")
print(" BRONZE STREAM SCHEMA")
print("========================================")

bronze_df.printSchema()


# ============================================================
# 7. DATA QUALITY - EXISTING IOT SENSORS
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
        col("temperature").isNull()
        | isnan(col("temperature"))
        | (col("temperature") < 0)
        | (col("temperature") > 150),
        "INVALID_TEMPERATURE"
    )

    .when(
        col("vibration").isNull()
        | isnan(col("vibration"))
        | (col("vibration") < 0)
        | (col("vibration") > 5),
        "INVALID_VIBRATION"
    )

    .when(
        col("pressure").isNull()
        | isnan(col("pressure"))
        | (col("pressure") < 0)
        | (col("pressure") > 20),
        "INVALID_PRESSURE"
    )

    .when(
        col("energy").isNull()
        | isnan(col("energy"))
        | (col("energy") < 0),
        "INVALID_ENERGY"
    )

    .otherwise("VALID")
)


# ============================================================
# 8. KEEP VALID IOT DATA
# ============================================================

clean_df = validated_df.filter(
    col("data_quality_status") == "VALID"
)


# ============================================================
# 9. ML DATA QUALITY
# ============================================================

ml_features = [
    "air_temperature",
    "process_temperature",
    "rotational_speed",
    "torque",
    "tool_wear"
]

missing_ml_feature = lit(False)

for feature in ml_features:

    missing_ml_feature = (
        missing_ml_feature
        | col(feature).isNull()
        | isnan(col(feature))
    )

ml_valid_range = (
    col("air_temperature").between(290, 315)
    & col("process_temperature").between(290, 330)
    & col("rotational_speed").between(1000, 3000)
    & col("torque").between(0, 100)
    & col("tool_wear").between(0, 300)
)

clean_df = clean_df.withColumn(

    "ml_data_quality_status",

    when(
        missing_ml_feature,
        "MISSING_ML_FEATURES"
    )

    .when(
        ~ml_valid_range,
        "INVALID_ML_FEATURES"
    )

    .otherwise("READY")
)


# ============================================================
# 10. ADD ALERT FEATURES
# ============================================================

silver_df = (

    clean_df

    .withColumn(
        "temperature_alert",
        when(
            col("temperature") > 85,
            True
        ).otherwise(False)
    )

    .withColumn(
        "vibration_alert",
        when(
            col("vibration") > 1.0,
            True
        ).otherwise(False)
    )

    .withColumn(
        "sensor_alert",
        when(
            (col("temperature") > 85)
            | (col("vibration") > 1.0),
            True
        ).otherwise(False)
    )

    .withColumn(
        "processing_timestamp",
        current_timestamp()
    )
)


# ============================================================
# 11. ALIGN DATAFRAME WITH ICEBERG TABLE SCHEMA
# ============================================================

# Iceberg ajoute les nouvelles colonnes a la fin des tables
# existantes. Il faut respecter l'ordre reel de la table.

table_schema = spark.table(table_name).schema

silver_df = silver_df.select(
    *[
        col(field.name)
        .cast(field.dataType)
        .alias(field.name)
        for field in table_schema.fields
    ]
)

print(
    "[OK] Ordre des colonnes Silver aligne avec Iceberg",
    flush=True
)


# ============================================================
# 12. PRINT FINAL SILVER SCHEMA
# ============================================================

print("\n========================================")
print(" SILVER STREAM SCHEMA")
print("========================================")

silver_df.printSchema()


# ============================================================
# 13. CHECKPOINT
# ============================================================

checkpoint_path = (
    "s3a://industrypulse/checkpoints/"
    "machine_sensors_silver"
)


# ============================================================
# 14. WRITE SILVER STREAM
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

    .toTable(table_name)
)


print("\n========================================")
print(" SILVER STREAMING STARTED")
print(" Bronze -> Spark -> Silver")
print("========================================\n")

query.awaitTermination()
