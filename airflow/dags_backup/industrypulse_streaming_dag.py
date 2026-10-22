
import json
import os
import socket
import urllib.error
import urllib.request

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task


RUNNER_URL = "http://spark:8090/run"


def check_tcp_service(host, port, service_name):
    try:
        with socket.create_connection((host, port), timeout=10):
            print(f"[OK] {service_name} accessible")
    except OSError as error:
        raise RuntimeError(
            f"[ERROR] {service_name} inaccessible : {error}"
        ) from error


def run_gold_sync():
    token = os.environ["SPARK_RUNNER_TOKEN"]

    payload = json.dumps({
        "job": "gold_sync"
    }).encode("utf-8")

    request = urllib.request.Request(
        RUNNER_URL,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "X-Runner-Token": token,
        },
        method="POST",
    )

    print("[INFO] Demarrage Gold Sync")

    try:
        with urllib.request.urlopen(
            request,
            timeout=1500,
        ) as response:
            result = json.loads(
                response.read().decode("utf-8")
            )

    except urllib.error.HTTPError as error:
        details = error.read().decode(
            "utf-8",
            errors="replace",
        )
        raise RuntimeError(
            f"Gold Sync HTTP {error.code}: {details[-12000:]}"
        ) from error

    if result.get("returncode") != 0:
        raise RuntimeError(
            f"Gold Sync a echoue : {result}"
        )

    print(result.get("output", ""))
    print("[SUCCESS] Gold Sync termine")


@dag(
    dag_id="industrypulse_streaming_pipeline",
    description="IndustryPulse Streaming - Monitoring, Gold Sync et KPI",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=[
        "industrypulse",
        "streaming",
        "spark",
        "timescaledb",
    ],
)
def industrypulse_streaming_pipeline():

    @task
    def check_mqtt():
        check_tcp_service(
            "mosquitto", 1883, "MQTT Mosquitto"
        )

    @task
    def check_kafka():
        check_tcp_service(
            "kafka", 29092, "Apache Kafka"
        )

    @task
    def check_spark_runner():
        check_tcp_service(
            "spark", 8090, "Spark Runner"
        )

    @task
    def check_timescaledb():
        check_tcp_service(
            "timescaledb", 5432, "TimescaleDB"
        )

    @task
    def sync_gold_to_timescaledb():
        run_gold_sync()

    @task
    def check_data_freshness():
        import psycopg2

        connection = psycopg2.connect(
            host="timescaledb",
            port=5432,
            dbname=os.getenv(
                "IOT_DB_NAME", "industrypulse"
            ),
            user=os.getenv(
                "IOT_DB_USER", "industrypulse"
            ),
            password=os.environ["IOT_DB_PASSWORD"],
            connect_timeout=10,
        )

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT MAX(window_end)
                    FROM public.machine_kpi
                    """
                )
                latest_timestamp = cursor.fetchone()[0]
        finally:
            connection.close()

        if latest_timestamp is None:
            raise RuntimeError(
                "Aucune donnee dans machine_kpi"
            )

        if latest_timestamp.tzinfo is None:
            latest_timestamp = latest_timestamp.replace(
                tzinfo=timezone.utc
            )

        now = datetime.now(timezone.utc)
        delay = now - latest_timestamp.astimezone(
            timezone.utc
        )

        print(f"[INFO] Derniere donnee : {latest_timestamp}")
        print(f"[INFO] Retard : {delay.total_seconds():.0f} secondes")

        if delay > timedelta(minutes=15):
            raise RuntimeError(
                "ALERTE : aucune nouvelle donnee IoT "
                "depuis plus de 15 minutes"
            )

        print("[SUCCESS] Donnees IoT recentes")

    mqtt = check_mqtt()
    kafka = check_kafka()
    spark = check_spark_runner()
    database = check_timescaledb()

    gold_sync = sync_gold_to_timescaledb()
    freshness = check_data_freshness()

    [mqtt, kafka, spark, database] >> gold_sync
    gold_sync >> freshness


industrypulse_streaming_pipeline()
