from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    round as spark_round
)


# ============================================================
# 1. SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("IndustryPulse-Batch-Processing")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. DATASET
# ============================================================

DATA_PATH = "/app/data/raw/ai4i2020.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(DATA_PATH)
)

print("\n========================================")
print(" IndustryPulse - Batch Processing")
print("========================================")

initial_count = df.count()

print(f"\nNombre initial de lignes : {initial_count}")


# ============================================================
# 3. VERIFICATION DES DOUBLONS
# ============================================================

duplicate_count = (
    df.count()
    - df.dropDuplicates().count()
)

print(f"Nombre de doublons : {duplicate_count}")


# ============================================================
# 4. SUPPRESSION DES DOUBLONS
# ============================================================

df_clean = df.dropDuplicates()

print(
    f"Nombre après suppression des doublons : "
    f"{df_clean.count()}"
)


# ============================================================
# 5. RENOMMAGE DES COLONNES
# ============================================================

column_mapping = {

    "UDI": "udi",

    "Product ID": "product_id",

    "Type": "product_type",

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

    "TWF": "tool_wear_failure",

    "HDF": "heat_dissipation_failure",

    "PWF": "power_failure",

    "OSF": "overstrain_failure",

    "RNF": "random_failure"
}


for old_name, new_name in column_mapping.items():

    df_clean = df_clean.withColumnRenamed(
        old_name,
        new_name
    )


print("\nColonnes après renommage :")

for column_name in df_clean.columns:
    print(f" - {column_name}")


# ============================================================
# 6. CONVERSION KELVIN -> CELSIUS
# ============================================================

df_clean = (
    df_clean

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
)


# ============================================================
# 7. DIFFERENCE DE TEMPERATURE
# ============================================================

df_clean = df_clean.withColumn(

    "temperature_difference",

    spark_round(
        col("process_temperature_c")
        - col("air_temperature_c"),
        2
    )
)


# ============================================================
# 8. PUISSANCE MECANIQUE APPROXIMATIVE
# ============================================================

# Power = Torque * angular velocity
#
# angular velocity =
# RPM * 2*pi / 60

df_clean = df_clean.withColumn(

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


# ============================================================
# 9. CREATION D'UN NIVEAU DE RISQUE SIMPLE
# ============================================================

df_clean = df_clean.withColumn(

    "risk_level",

    when(
        col("machine_failure") == 1,
        "CRITICAL"
    )

    .when(
        (col("tool_wear_min") > 200)
        | (col("torque_nm") > 60),
        "WARNING"
    )

    .otherwise("NORMAL")
)


# ============================================================
# 10. VERIFICATION DES VALEURS IMPOSSIBLES
# ============================================================

invalid_data = df_clean.filter(

    (col("rotational_speed_rpm") <= 0)

    | (col("torque_nm") < 0)

    | (col("tool_wear_min") < 0)

)


invalid_count = invalid_data.count()

print(
    f"\nValeurs physiquement incohérentes : "
    f"{invalid_count}"
)


# ============================================================
# 11. SUPPRESSION DES VALEURS INVALIDES
# ============================================================

df_clean = df_clean.filter(

    (col("rotational_speed_rpm") > 0)

    & (col("torque_nm") >= 0)

    & (col("tool_wear_min") >= 0)

)


# ============================================================
# 12. RESULTAT
# ============================================================

print("\n========================================")
print(" Dataset après traitement")
print("========================================")

print(
    f"Nombre final de lignes : "
    f"{df_clean.count()}"
)

print(
    f"Nombre de colonnes : "
    f"{len(df_clean.columns)}"
)


df_clean.printSchema()


# ============================================================
# 13. APERCU
# ============================================================

df_clean.select(

    "product_id",
    "product_type",

    "air_temperature_c",
    "process_temperature_c",

    "rotational_speed_rpm",
    "torque_nm",
    "tool_wear_min",

    "mechanical_power_w",

    "machine_failure",

    "risk_level"

).show(
    20,
    truncate=False
)


# ============================================================
# 14. ANALYSE DES PANNES
# ============================================================

print("\n--- Répartition Machine Failure ---")

df_clean.groupBy(
    "machine_failure"
).count().show()


print("\n--- Répartition Risk Level ---")

df_clean.groupBy(
    "risk_level"
).count().show()


print("\n--- Pannes par type de produit ---")

df_clean.groupBy(
    "product_type",
    "machine_failure"
).count().orderBy(
    "product_type",
    "machine_failure"
).show()


# ============================================================
# FIN
# ============================================================

print("\nBatch Processing terminé avec succès.")

spark.stop()