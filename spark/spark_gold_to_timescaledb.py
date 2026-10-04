from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# ============================================================
# 1. SPARK + ICEBERG
# ============================================================

spark = (
    SparkSession.builder
    .appName("IndustryPulse-Gold-To-TimescaleDB")

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
# 2. MINIO / S3A CONFIGURATION
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
# 3. READ GOLD ICEBERG TABLE
# ============================================================

print("\n========================================")
print(" READING GOLD ICEBERG TABLE")
print("========================================")

gold_df = (
    spark.read
    .format("iceberg")
    .load("industrypulse.gold.machine_kpi")
)


# ============================================================
# 4. SELECT SERVING COLUMNS
# ============================================================

serving_df = gold_df.select(
    col("machine_id"),
    col("window_start"),
    col("window_end"),
    col("total_measurements"),
    col("avg_temperature"),
    col("min_temperature"),
    col("max_temperature"),
    col("avg_vibration"),
    col("max_vibration"),
    col("avg_pressure"),
    col("avg_energy"),
    col("total_alerts"),
    col("alert_rate"),
    col("gold_processing_timestamp")
)


# ============================================================
# 5. VERIFY DATA
# ============================================================

row_count = serving_df.count()

print("\n========================================")
print(f" GOLD ROWS FOUND : {row_count}")
print("========================================\n")

serving_df.show(
    20,
    truncate=False
)


# ============================================================
# 6. TIMESCALEDB JDBC CONFIGURATION
# ============================================================

jdbc_url = (
    "jdbc:postgresql://timescaledb:5432/industrypulse"
)

jdbc_properties = {
    "user": "industrypulse",
    "password": "industrypulse123",
    "driver": "org.postgresql.Driver"
}


# ============================================================
# 7. WRITE GOLD KPI TO TIMESCALEDB
# ============================================================

print("\n========================================")
print(" WRITING GOLD -> TIMESCALEDB")
print("========================================")

serving_df.write \
    .mode("append") \
    .jdbc(
        url=jdbc_url,
        table="public.machine_kpi",
        properties=jdbc_properties
    )


print("\n========================================")
print(" GOLD -> TIMESCALEDB COMPLETED")
print(f" ROWS EXPORTED : {row_count}")
print("========================================")


spark.stop()