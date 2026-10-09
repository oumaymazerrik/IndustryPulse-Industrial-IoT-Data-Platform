
import os
import socket
import urllib.request
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task


def check_tcp_service(host, port, service_name):
    """Vérifie la disponibilité réseau d'un service."""
    try:
        with socket.create_connection((host, port), timeout=10):
            print(f"[OK] {service_name} accessible sur {host}:{port}")
    except OSError as error:
        raise RuntimeError(
            f"[ERROR] {service_name} inaccessible : {error}"
        ) from error


@dag(
    dag_id="industrypulse_streaming_monitor",
    description="Monitoring du pipeline IoT Streaming IndustryPulse",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=2),
    },
    tags=["industrypulse", "streaming", "monitoring"],
)
def streaming_monitor():

    @task
    def check_mqtt():
        check_tcp_service("mosquitto", 1883, "MQTT Mosquitto")

    @task
    def check_kafka():
        check_tcp_service("kafka", 29092, "Apache Kafka")

    @task
    def check_spark_runner():
        check_tcp_service("spark", 8090, "Spark Runner")

    @task
    def check_timescaledb():
        check_tcp_service("timescaledb", 5432, "TimescaleDB")

    @task
    def check_data_freshness():
        import psycopg2

        connection = psycopg2.connect(
            host="timescaledb",
            port=5432,
            dbname=os.environ.get("IOT_DB_NAME", "industrypulse"),
            user=os.environ.get("IOT_DB_USER", "industrypulse"),
            password=os.environ["IOT_DB_PASSWORD"],
            connect_timeout=10,
        )

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT MAX(time) FROM public.machine_kpi"
                )
                latest_timestamp = cursor.fetchone()[0]
        finally:
            connection.close()

        if latest_timestamp is None:
            raise RuntimeError(
                "Aucune donnée IoT dans machine_kpi"
            )

        if latest_timestamp.tzinfo is None:
            latest_timestamp = latest_timestamp.replace(
                tzinfo=timezone.utc
            )

        now = datetime.now(timezone.utc)
        delay = now - latest_timestamp.astimezone(timezone.utc)

        print(f"Dernière donnée : {latest_timestamp}")
        print(f"Retard : {delay.total_seconds():.0f} secondes")

        if delay > timedelta(minutes=5):
            raise RuntimeError(
                "ALERTE : aucune nouvelle donnée IoT "
                "depuis plus de 5 minutes"
            )

        print("[OK] Données IoT récentes")

    mqtt = check_mqtt()
    kafka = check_kafka()
    spark = check_spark_runner()
    database = check_timescaledb()

    freshness = check_data_freshness()

    [mqtt, kafka, spark, database] >> freshness


streaming_monitor()
