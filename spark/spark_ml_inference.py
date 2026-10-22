import json
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

API_URL = "http://industrypulse-ml-api:8000/predict"

spark = (
    SparkSession.builder
    .appName("IndustryPulse-ML-Inference-Test")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config(
        "spark.sql.catalog.lakehouse.warehouse",
        "s3a://industrypulse/warehouse",
    )
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("[INFO] Lecture des donnees Silver...")

data = (
    spark.read.table("lakehouse.silver.machine_sensors_clean")
    .filter(col("ml_data_quality_status") == "READY")
    .select(
        "machine_id",
        "event_timestamp",
        "air_temperature",
        "process_temperature",
        "rotational_speed",
        "torque",
        "tool_wear",
    )
    .orderBy(col("event_timestamp").desc())
    .limit(5)
    .collect()
)

print(f"[INFO] {len(data)} mesures selectionnees")

for row in data:
    payload = {
        "air_temperature": row.air_temperature,
        "process_temperature": row.process_temperature,
        "rotational_speed": row.rotational_speed,
        "torque": row.torque,
        "tool_wear": row.tool_wear,
    }

    request = urllib.request.Request(
        API_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            prediction = json.loads(response.read().decode("utf-8"))

        print(
            f"[PREDICTION] {row.machine_id} | "
            f"{row.event_timestamp} | "
            f"Probability={prediction['failure_probability']} | "
            f"Status={prediction['status']}"
        )

    except Exception as error:
        print(f"[ERROR] {row.machine_id}: {error}")

spark.stop()