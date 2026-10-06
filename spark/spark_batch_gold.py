from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# Spark + Iceberg + MinIO
# ============================================================

spark = (
    SparkSession.builder
    .appName("IndustryPulse-AI4I-Gold")
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


print("\n============================================")
print("===== INDUSTRYPULSE AI4I GOLD BATCH ========")
print("============================================\n")


# ============================================================
# 1. Read Silver AI4I
# ============================================================

silver_df = spark.table(
    "industrypulse.silver.ai4i_clean"
)

silver_count = silver_df.count()

print(f"Silver rows : {silver_count}")


# ============================================================
# 2. Create Gold namespace
# ============================================================

spark.sql(
    "CREATE NAMESPACE IF NOT EXISTS industrypulse.gold"
)


# ============================================================
# GOLD TABLE 1
# Failure Summary by Product Type
# ============================================================

failure_summary = (
    silver_df
    .groupBy("product_type")
    .agg(

        F.count("*").alias(
            "total_records"
        ),

        F.sum("machine_failure").alias(
            "total_failures"
        ),

        F.round(
            F.avg("machine_failure") * 100,
            2
        ).alias(
            "failure_rate"
        ),

        F.round(
            F.avg("air_temperature_c"),
            2
        ).alias(
            "avg_air_temperature_c"
        ),

        F.round(
            F.avg("process_temperature_c"),
            2
        ).alias(
            "avg_process_temperature_c"
        ),

        F.round(
            F.avg("rotational_speed_rpm"),
            2
        ).alias(
            "avg_rotational_speed_rpm"
        ),

        F.round(
            F.avg("torque_nm"),
            2
        ).alias(
            "avg_torque_nm"
        ),

        F.round(
            F.avg("tool_wear_min"),
            2
        ).alias(
            "avg_tool_wear_min"
        )
    )
)


# ============================================================
# 3. Display Failure Summary
# ============================================================

print("\n===== FAILURE SUMMARY =====\n")

failure_summary \
    .orderBy("product_type") \
    .show(truncate=False)


# ============================================================
# 4. Write Gold Table 1
# ============================================================

(
    failure_summary
    .writeTo(
        "industrypulse.gold.ai4i_failure_summary"
    )
    .using("iceberg")
    .createOrReplace()
)

print(
    "\nGold table created: "
    "industrypulse.gold.ai4i_failure_summary"
)


# ============================================================
# GOLD TABLE 2
# Failure Types by Product Type
# ============================================================

failure_types = (
    silver_df
    .groupBy("product_type")
    .agg(

        F.count("*").alias(
            "total_records"
        ),

        F.sum("machine_failure").alias(
            "total_machine_failures"
        ),

        F.sum("tool_wear_failure").alias(
            "tool_wear_failures"
        ),

        F.sum("heat_dissipation_failure").alias(
            "heat_dissipation_failures"
        ),

        F.sum("power_failure").alias(
            "power_failures"
        ),

        F.sum("overstrain_failure").alias(
            "overstrain_failures"
        ),

        F.sum("random_failure").alias(
            "random_failures"
        )
    )
)


# ============================================================
# 5. Display Failure Types
# ============================================================

print(
    "\n===== FAILURE TYPES BY PRODUCT TYPE =====\n"
)

failure_types \
    .orderBy("product_type") \
    .show(truncate=False)


# ============================================================
# 6. Write Gold Table 2
# ============================================================

(
    failure_types
    .writeTo(
        "industrypulse.gold.ai4i_failure_types"
    )
    .using("iceberg")
    .createOrReplace()
)

print(
    "\nGold table created: "
    "industrypulse.gold.ai4i_failure_types"
)


# ============================================================
# GOLD TABLE 3
# Operating Conditions: Normal vs Failure
# ============================================================

failure_conditions = (
    silver_df

    .withColumn(
        "machine_status",
        F.when(
            F.col("machine_failure") == 1,
            "FAILURE"
        ).otherwise("NORMAL")
    )

    .groupBy("machine_status")

    .agg(

        F.count("*").alias(
            "total_records"
        ),

        F.round(
            F.avg("air_temperature_c"),
            2
        ).alias(
            "avg_air_temperature_c"
        ),

        F.round(
            F.avg("process_temperature_c"),
            2
        ).alias(
            "avg_process_temperature_c"
        ),

        F.round(
            F.avg("temperature_difference"),
            2
        ).alias(
            "avg_temperature_difference"
        ),

        F.round(
            F.avg("rotational_speed_rpm"),
            2
        ).alias(
            "avg_rotational_speed_rpm"
        ),

        F.round(
            F.avg("torque_nm"),
            2
        ).alias(
            "avg_torque_nm"
        ),

        F.round(
            F.avg("tool_wear_min"),
            2
        ).alias(
            "avg_tool_wear_min"
        ),

        F.round(
            F.avg("mechanical_power_w"),
            2
        ).alias(
            "avg_mechanical_power_w"
        )
    )
)


# ============================================================
# 7. Display Failure Conditions
# ============================================================

print(
    "\n===== NORMAL VS FAILURE CONDITIONS =====\n"
)

failure_conditions \
    .orderBy("machine_status") \
    .show(truncate=False)


# ============================================================
# 8. Write Gold Table 3
# ============================================================

(
    failure_conditions
    .writeTo(
        "industrypulse.gold.ai4i_failure_conditions"
    )
    .using("iceberg")
    .createOrReplace()
)

print(
    "\nGold table created: "
    "industrypulse.gold.ai4i_failure_conditions"
)


# ============================================================
# Final Summary
# ============================================================

print("\n============================================")
print("GOLD BATCH COMPLETED")
print("============================================")

print(
    "1. industrypulse.gold.ai4i_failure_summary"
)

print(
    "2. industrypulse.gold.ai4i_failure_types"
)

print(
    "3. industrypulse.gold.ai4i_failure_conditions"
)

print("============================================\n")


# ============================================================
# Stop Spark
# ============================================================

spark.stop()