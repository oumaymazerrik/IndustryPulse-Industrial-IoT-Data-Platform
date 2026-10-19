
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
# 1. CONFIGURATION
# ============================================================

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = "machine-sensors"

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "http://minio:9000"
)

MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ROOT_USER",
    os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
)

MINIO_SECRET_KEY = os.getenv(
    "MINIO_ROOT_PASSWORD",
    os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
)

BRONZE_TABLE = "lakehouse.bronze.machine_sensors_raw"

CHECKPOINT_PATH = (
    "s3a://industrypulse/checkpoints/"
    "machine_sensors_bronze_v3"
)


# ============================================================
# 2. SPARK + ICEBERG + MINIO
# ============================================================

spark = (
    SparkSession.builder
    .appName("IndustryPulse-Streaming-Iceberg")
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
        MINIO_ENDPOINT
    )
    .config(
        "spark.hadoop.fs.s3a.access.key",
        MINIO_ACCESS_KEY
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        MINIO_SECRET_KEY
    )
    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true"
    )
    .config(
        "spark.hadoop.fs.s3a.connection.ssl.enabled",
        "false"
    )
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 3. CREATE ICEBERG NAMESPACE
# ============================================================

spark.sql(
    "CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze"
)


# ============================================================
# 4. IOT JSON SCHEMA
# ============================================================

iot_schema = StructType([
    StructField("machine_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("energy", DoubleType(), True),

    # Variables synthetiques pour XGBoost
    StructField("air_temperature", DoubleType(), True),
    StructField("process_temperature", DoubleType(), True),
    StructField("rotational_speed", DoubleType(), True),
    StructField("torque", DoubleType(), True),
    StructField("tool_wear", DoubleType(), True),

    StructField("status", StringType(), True),
    StructField("timestamp", StringType(), True)
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
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)


# ============================================================
# 6. PARSE KAFKA JSON
# ============================================================

parsed_df = (
    kafka_df
    .select(
        from_json(
            col("value").cast("string"),
            iot_schema
        ).alias("data"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp")
    )
)


# ============================================================
# 7. CREATE BRONZE DATAFRAME
# ============================================================

bronze_df = parsed_df.select(
    col("data.machine_id").alias("machine_id"),
    col("data.temperature").alias("temperature"),
    col("data.vibration").alias("vibration"),
    col("data.pressure").alias("pressure"),
    col("data.energy").alias("energy"),

    col("data.air_temperature").alias("air_temperature"),
    col("data.process_temperature").alias("process_temperature"),
    col("data.rotational_speed").alias("rotational_speed"),
    col("data.torque").alias("torque"),
    col("data.tool_wear").alias("tool_wear"),

    col("data.status").alias("status"),
    to_timestamp(col("data.timestamp")).alias("event_timestamp"),
    col("kafka_partition"),
    col("kafka_offset"),
    col("kafka_timestamp"),
    current_timestamp().alias("ingestion_timestamp")
)


# ============================================================
# 8. CREATE OR EVOLVE ICEBERG BRONZE TABLE
# ============================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE}
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
    air_temperature DOUBLE,
    process_temperature DOUBLE,
    rotational_speed DOUBLE,
    torque DOUBLE,
    tool_wear DOUBLE
)
USING iceberg
""")

existing_columns = {
    field.name
    for field in spark.table(BRONZE_TABLE).schema.fields
}

new_columns = {
    "air_temperature": "DOUBLE",
    "process_temperature": "DOUBLE",
    "rotational_speed": "DOUBLE",
    "torque": "DOUBLE",
    "tool_wear": "DOUBLE"
}

for column_name, column_type in new_columns.items():
    if column_name not in existing_columns:
        spark.sql(
            f"ALTER TABLE {BRONZE_TABLE} "
            f"ADD COLUMN {column_name} {column_type}"
        )
        print(f"[OK] Colonne ajoutee : {column_name}")

print("[OK] Schema Iceberg Bronze pret")


# ============================================================
# 9. ALIGN DATAFRAME WITH ICEBERG TABLE
# ============================================================

# Important :
# Les nouvelles colonnes ML ont ete ajoutees a la fin
# de la table Iceberg historique.
#
# Le DataFrame doit respecter exactement le meme ordre.

table_schema = spark.table(BRONZE_TABLE).schema

table_columns = [
    field.name
    for field in table_schema.fields
]

# Verifier que toutes les colonnes de la table existent
# dans le DataFrame avant de lancer le streaming.

missing_columns = [
    column_name
    for column_name in table_columns
    if column_name not in bronze_df.columns
]

if missing_columns:
    raise ValueError(
        "Colonnes absentes du DataFrame Bronze : "
        + ", ".join(missing_columns)
    )

# Reorganiser automatiquement les colonnes.
# Respecter egalement les types de la table Iceberg.

bronze_df = bronze_df.select(
    *[
        col(field.name).cast(field.dataType).alias(field.name)
        for field in table_schema.fields
    ]
)

print("[OK] Ordre des colonnes aligne avec Iceberg")


# ============================================================
# 10. DISPLAY FINAL BRONZE SCHEMA
# ============================================================

print("\n========================================")
print(" BRONZE STREAM SCHEMA")
print("========================================")

bronze_df.printSchema()


# ============================================================
# 11. WRITE STREAM TO ICEBERG
# ============================================================

query = (
    bronze_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )
    .trigger(processingTime="10 seconds")
    .toTable(BRONZE_TABLE)
)

print("\n========================================")
print(" STREAMING STARTED")
print(" Kafka -> Spark -> Iceberg Bronze")
print("========================================\n")

query.awaitTermination()
