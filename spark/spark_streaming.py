from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("IndustryPulse-Streaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# SCHEMA DES DONNEES IoT
# ============================================================

schema = StructType([
    StructField("machine_id", StringType(), False),
    StructField("temperature", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("energy", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", StringType(), True)
])


# ============================================================
# LECTURE DE KAFKA
# ============================================================

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
# KAFKA VALUE : BINARY -> STRING
# ============================================================

json_df = kafka_df.select(
    col("key").cast("string").alias("kafka_key"),
    col("value").cast("string").alias("json_value"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp")
)


# ============================================================
# PARSING JSON
# ============================================================

parsed_df = (
    json_df
    .select(
        from_json(
            col("json_value"),
            schema
        ).alias("data"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp")
    )
)


# ============================================================
# DATAFRAME STRUCTURE
# ============================================================

machines_df = parsed_df.select(
    col("data.machine_id"),
    col("data.temperature"),
    col("data.vibration"),
    col("data.pressure"),
    col("data.energy"),
    col("data.status"),
    col("data.timestamp"),
    col("partition"),
    col("offset")
)


# ============================================================
# AFFICHAGE DU SCHEMA
# ============================================================

print("\n==========================================")
print(" IndustryPulse - Spark Structured Streaming")
print("==========================================")

machines_df.printSchema()


# ============================================================
# STREAMING -> CONSOLE
# ============================================================

query = (
    machines_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)


# ============================================================
# ATTENDRE LES EVENEMENTS
# ============================================================

query.awaitTermination()