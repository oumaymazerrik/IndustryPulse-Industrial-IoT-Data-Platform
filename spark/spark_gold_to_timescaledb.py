
import os
import uuid

import psycopg2

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# CONFIGURATION
# ============================================================

MINIO_USER = os.environ["MINIO_ROOT_USER"]
MINIO_PASSWORD = os.environ["MINIO_ROOT_PASSWORD"]
MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "http://minio:9000"
)

DB_HOST = os.getenv("IOT_DB_HOST", "timescaledb")
DB_PORT = int(os.getenv("IOT_DB_PORT", "5432"))
DB_NAME = os.getenv("IOT_DB_NAME", "industrypulse")
DB_USER = os.getenv("IOT_DB_USER", "industrypulse")
DB_PASSWORD = os.environ["IOT_DB_PASSWORD"]

JDBC_URL = (
    f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

TARGET_TABLE = "public.machine_kpi"

GOLD_TABLE = os.getenv(
    "ICEBERG_GOLD_TABLE",
    "lakehouse.gold.machine_kpi"
)

COLUMNS = [
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
    "gold_processing_timestamp",
]

KEY_COLUMNS = [
    "machine_id",
    "window_start",
]


# ============================================================
# POSTGRESQL CONNECTION
# ============================================================

def get_postgres_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=15,
    )


def execute_sql(sql):
    """
    Execute une instruction SQL PostgreSQL.

    La transaction est validee automatiquement
    si tout se passe correctement.

    En cas d'erreur, elle est annulee.
    """
    connection = get_postgres_connection()

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)

    finally:
        connection.close()


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("IndustryPulse-Gold-To-TimescaleDB")

        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions."
            "IcebergSparkSessionExtensions"
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
            MINIO_USER
        )

        .config(
            "spark.hadoop.fs.s3a.secret.key",
            MINIO_PASSWORD
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

    return spark


# ============================================================
# GOLD -> TIMESCALEDB
# ============================================================

def main():
    spark = None

    staging_table = (
        "public.machine_kpi_staging_"
        + uuid.uuid4().hex[:12]
    )

    staging_created = False

    try:
        print("=" * 60)
        print("INDUSTRYPULSE - GOLD TO TIMESCALEDB")
        print("=" * 60)

        # ----------------------------------------------------
        # 1. Spark Session
        # ----------------------------------------------------

        spark = create_spark_session()

        print(f"[INFO] Lecture Iceberg : {GOLD_TABLE}")

        gold_df = spark.table(GOLD_TABLE)

        # ----------------------------------------------------
        # 2. Selection des colonnes
        # ----------------------------------------------------

        gold_df = gold_df.select(
            *[F.col(column) for column in COLUMNS]
        )

        gold_df = gold_df.dropna(
            subset=KEY_COLUMNS
        )

        # ----------------------------------------------------
        # 3. Deduplication des cles primaires
        # ----------------------------------------------------

        window_spec = (
            Window
            .partitionBy(*KEY_COLUMNS)
            .orderBy(
                F.col(
                    "gold_processing_timestamp"
                ).desc_nulls_last()
            )
        )

        gold_df = (
            gold_df
            .withColumn(
                "_row_number",
                F.row_number().over(window_spec)
            )
            .filter(
                F.col("_row_number") == 1
            )
            .drop("_row_number")
        )

        total_rows = gold_df.count()

        print(f"[INFO] Lignes Gold : {total_rows}")

        if total_rows == 0:
            print("[INFO] Aucune donnee a synchroniser.")
            return

        # ----------------------------------------------------
        # 4. Creation de la table staging
        # ----------------------------------------------------

        print(
            f"[INFO] Creation staging : {staging_table}"
        )

        execute_sql(
            f"""
            CREATE TABLE {staging_table}
            (
                LIKE {TARGET_TABLE}
                EXCLUDING ALL
            )
            """
        )

        staging_created = True

        # ----------------------------------------------------
        # 5. Spark JDBC -> staging
        # ----------------------------------------------------

        print("[INFO] Chargement JDBC vers staging...")

        (
            gold_df.write
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", staging_table)
            .option("user", DB_USER)
            .option("password", DB_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .option("batchsize", "1000")
            .mode("append")
            .save()
        )

        # ----------------------------------------------------
        # 6. UPSERT staging -> machine_kpi
        # ----------------------------------------------------

        print(
            "[INFO] UPSERT vers public.machine_kpi..."
        )

        column_list = ", ".join(COLUMNS)

        update_columns = [
            column
            for column in COLUMNS
            if column not in KEY_COLUMNS
        ]

        update_clause = ",\n".join(
            f"{column} = EXCLUDED.{column}"
            for column in update_columns
        )

        execute_sql(
            f"""
            INSERT INTO {TARGET_TABLE}
                ({column_list})
            SELECT
                {column_list}
            FROM {staging_table}
            ON CONFLICT (machine_id, window_start)
            DO UPDATE SET
                {update_clause}
            """
        )

        print(
            "[SUCCESS] Synchronisation Gold -> TimescaleDB terminee."
        )

    except Exception as error:
        print(
            f"[ERROR] Echec de synchronisation : {error}"
        )
        raise

    finally:
        # ----------------------------------------------------
        # 7. Nettoyage de la table staging
        # ----------------------------------------------------

        if staging_created:
            try:
                print(
                    "[INFO] Suppression de la table staging."
                )

                execute_sql(
                    f"DROP TABLE IF EXISTS {staging_table}"
                )

            except Exception as cleanup_error:
                print(
                    f"[WARNING] Nettoyage impossible : "
                    f"{cleanup_error}"
                )

        if spark is not None:
            spark.stop()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()

