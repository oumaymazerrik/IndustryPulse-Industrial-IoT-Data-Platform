from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    round as spark_round
)


# ============================================================
# 1. SPARK + ICEBERG
# ============================================================

spark = (
    SparkSession.builder
    .appName("IndustryPulse-Iceberg-Batch")

    # Iceberg SQL extensions
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # Catalog
    .config(
        "spark.sql.catalog.industrypulse",
        "org.apache.iceberg.spark.SparkCatalog"
    )

    # Hadoop Catalog
    .config(
        "spark.sql.catalog.industrypulse.type",
        "hadoop"
    )

    # Warehouse dans MinIO
    .config(
        "spark.sql.catalog.industrypulse.warehouse",
        "s3a://industrypulse/warehouse"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. CONFIGURATION S3A / MINIO
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


# ============================================================
# 3. CREATE MEDALLION NAMESPACES
# ============================================================

print("\n========================================")
print(" CREATE ICEBERG NAMESPACES")
print("========================================")

spark.sql(
    "CREATE NAMESPACE IF NOT EXISTS industrypulse.bronze"
)

spark.sql(
    "CREATE NAMESPACE IF NOT EXISTS industrypulse.silver"
)

spark.sql(
    "CREATE NAMESPACE IF NOT EXISTS industrypulse.gold"
)

print("Namespaces Bronze / Silver / Gold créés.")


# ============================================================
# 4. READ RAW CSV
# ============================================================

DATA_PATH = "/app/data/raw/ai4i2020.csv"

raw_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(DATA_PATH)
)

print("\n========================================")
print(" RAW DATASET")
print("========================================")

print(
    f"Nombre de lignes : {raw_df.count()}"
)

raw_df.printSchema()


# ============================================================
# 5. WRITE BRONZE ICEBERG
# ============================================================

print("\n========================================")
print(" WRITE BRONZE")
print("========================================")

(
    raw_df.writeTo(
        "industrypulse.bronze.ai4i_raw"
    )
    .using("iceberg")
    .createOrReplace()
)

print(
    "Table industrypulse.bronze.ai4i_raw créée."
)


# ============================================================
# 6. READ BRONZE
# ============================================================

bronze_df = spark.table(
    "industrypulse.bronze.ai4i_raw"
)

print("\nBRONZE :")

bronze_df.show(
    5,
    truncate=False
)

print(
    f"Nombre Bronze : {bronze_df.count()}"
)


# ============================================================
# 7. CLEANING / SILVER
# ============================================================

silver_df = bronze_df.dropDuplicates()


column_mapping = {

    "UDI":
        "udi",

    "Product ID":
        "product_id",

    "Type":
        "product_type",

    "Air temperature [K]":
        "air_temperature_k",

    "Process temperature [K]":
        "process_temperature_k",

    "Rotational speed [rpm]":
        "rotational_speed_rpm",

    "Torque [Nm]":
        "torque_nm",

    "Tool wear [min]":
        "tool_wear_min",

    "Machine failure":
        "machine_failure",

    "TWF":
        "tool_wear_failure",

    "HDF":
        "heat_dissipation_failure",

    "PWF":
        "power_failure",

    "OSF":
        "overstrain_failure",

    "RNF":
        "random_failure"
}


for old_name, new_name in column_mapping.items():

    silver_df = silver_df.withColumnRenamed(
        old_name,
        new_name
    )


# ============================================================
# 8. FEATURE ENGINEERING
# ============================================================

silver_df = (
    silver_df

    .withColumn(
        "air_temperature_c",
        spark_round(
            col("air_temperature_k") - 273.15,
            2
        )
    )

    .withColumn(
        "process_temperature_c",
        spark_round(
            col("process_temperature_k") - 273.15,
            2
        )
    )

    .withColumn(
        "temperature_difference",
        spark_round(
            col("process_temperature_k")
            - col("air_temperature_k"),
            2
        )
    )

    .withColumn(
        "mechanical_power_w",
        spark_round(
            col("torque_nm")
            * col("rotational_speed_rpm")
            * 2
            * 3.14159265359
            / 60,
            2
        )
    )
)


# ============================================================
# 9. DATA QUALITY
# ============================================================

silver_df = silver_df.filter(

    (col("rotational_speed_rpm") > 0)

    & (col("torque_nm") >= 0)

    & (col("tool_wear_min") >= 0)

)


# ============================================================
# 10. WRITE SILVER ICEBERG
# ============================================================

print("\n========================================")
print(" WRITE SILVER")
print("========================================")

(
    silver_df.writeTo(
        "industrypulse.silver.ai4i_clean"
    )
    .using("iceberg")
    .createOrReplace()
)

print(
    "Table industrypulse.silver.ai4i_clean créée."
)


# ============================================================
# 11. VALIDATION
# ============================================================

print("\n========================================")
print(" ICEBERG TABLES")
print("========================================")

spark.sql(
    "SHOW TABLES IN industrypulse.bronze"
).show(truncate=False)

spark.sql(
    "SHOW TABLES IN industrypulse.silver"
).show(truncate=False)


print("\n========================================")
print(" SILVER DATA")
print("========================================")

spark.table(
    "industrypulse.silver.ai4i_clean"
).select(

    "product_id",
    "product_type",
    "air_temperature_c",
    "process_temperature_c",
    "rotational_speed_rpm",
    "torque_nm",
    "mechanical_power_w",
    "machine_failure"

).show(
    10,
    truncate=False
)


print("\n========================================")
print(" INDUSTRYPULSE LAKEHOUSE : OK")
print("========================================")


spark.stop()