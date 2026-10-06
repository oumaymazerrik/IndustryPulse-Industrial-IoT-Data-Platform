from pyspark.sql import SparkSession


# ============================================================
# Spark + Iceberg + MinIO
# ============================================================

spark = (
    SparkSession.builder
    .appName("IndustryPulse-AI4I-Gold-To-TimescaleDB")

    # Iceberg catalog
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

    # MinIO / S3A
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        "http://minio:9000"
    )
    .config(
        "spark.hadoop.fs.s3a.access.key",
        "admin"
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        "industrypulse123"
    )
    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true"
    )
    .config(
        "spark.hadoop.fs.s3a.connection.ssl.enabled",
        "false"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("\n==============================================")
print("===== AI4I GOLD -> TIMESCALEDB EXPORT ========")
print("==============================================\n")


# ============================================================
# PostgreSQL / TimescaleDB
# ============================================================

jdbc_url = "jdbc:postgresql://timescaledb:5432/industrypulse"

jdbc_properties = {
    "user": "industrypulse",
    "password": "industrypulse123",
    "driver": "org.postgresql.Driver"
}


# ============================================================
# Export function
# ============================================================

def export_table(source_table, target_table):

    print("\n----------------------------------------------")
    print(f"Reading Iceberg table : {source_table}")
    print("----------------------------------------------")

    df = spark.table(source_table)

    row_count = df.count()

    print(f"Rows found : {row_count}")

    df.show(truncate=False)

    print(f"\nExporting to PostgreSQL : {target_table}")

    (
        df.write
        .mode("overwrite")
        .jdbc(
            url=jdbc_url,
            table=target_table,
            properties=jdbc_properties
        )
    )

    print(f"Export completed : {target_table}")


# ============================================================
# Gold Table 1
# ============================================================

export_table(
    "industrypulse.gold.ai4i_failure_summary",
    "public.ai4i_failure_summary"
)


# ============================================================
# Gold Table 2
# ============================================================

export_table(
    "industrypulse.gold.ai4i_failure_types",
    "public.ai4i_failure_types"
)


# ============================================================
# Gold Table 3
# ============================================================

export_table(
    "industrypulse.gold.ai4i_failure_conditions",
    "public.ai4i_failure_conditions"
)


# ============================================================
# Final summary
# ============================================================

print("\n==============================================")
print("AI4I GOLD -> TIMESCALEDB COMPLETED")
print("==============================================")
print("1. public.ai4i_failure_summary")
print("2. public.ai4i_failure_types")
print("3. public.ai4i_failure_conditions")
print("==============================================\n")


spark.stop()