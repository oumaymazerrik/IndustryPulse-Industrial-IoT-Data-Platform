import json
import os
import socket
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

RUNNER_URL = "http://spark:8090/run"


def tcp_check(host, port):
    with socket.create_connection((host, port), timeout=10):
        print(f"[OK] {host}:{port}")


def run_gold_sync():
    payload = json.dumps({"job": "gold_sync"}).encode()
    request = urllib.request.Request(
        RUNNER_URL, data=payload, method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Runner-Token": os.environ["SPARK_RUNNER_TOKEN"],
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=1500) as response:
            result = json.loads(response.read().decode())
    except urllib.error.HTTPError as error:
        details = error.read().decode(errors="replace")
        raise RuntimeError(f"Gold Sync HTTP {error.code}: {details[-10000:]}") from error
    if result.get("returncode") != 0:
        raise RuntimeError(f"Gold Sync failed: {result}")
    print("[SUCCESS] Gold Sync", result.get("output", "")[-3000:])


@dag(
    dag_id="industrypulse_streaming_pipeline",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1),
                  "execution_timeout": timedelta(minutes=30)},
    tags=["industrypulse", "streaming", "timescaledb"],
)
def industrypulse_streaming_pipeline():
    @task
    def check_services():
        for host, port in [
            ("mosquitto", 1883), ("kafka", 29092),
            ("spark", 8090), ("timescaledb", 5432),
        ]:
            tcp_check(host, port)

    @task
    def check_streaming_processes():
        # Chaque Spark driver expose son UI HTTP sur 4040.
        # La disponibilité HTTP ne prouve pas la progression des données ;
        # le contrôle de fraîcheur final complète cette vérification.
        for host in ("spark-bronze-stream", "spark-silver-stream", "spark-gold-stream"):
            try:
                with urllib.request.urlopen(f"http://{host}:4040/api/v1/applications", timeout=10) as response:
                    applications = json.loads(response.read().decode())
                if not applications:
                    raise RuntimeError(f"Spark UI sans application: {host}")
                print(f"[OK] Spark actif : {host}")
            except (OSError, ValueError) as error:
                raise RuntimeError(f"Spark Streaming indisponible: {host}: {error}") from error

    @task
    def sync_gold_to_timescaledb():
        run_gold_sync()

    @task
    def check_data_freshness():
        import psycopg2
        connection = psycopg2.connect(
            host="timescaledb", port=5432,
            dbname=os.getenv("IOT_DB_NAME", "industrypulse"),
            user=os.getenv("IOT_DB_USER", "industrypulse"),
            password=os.environ["IOT_DB_PASSWORD"],
            connect_timeout=10,
        )
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT MAX(window_end) FROM public.machine_kpi")
                latest = cursor.fetchone()[0]
        finally:
            connection.close()
        if latest is None:
            raise RuntimeError("Aucun KPI dans machine_kpi")
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        delay = datetime.now(timezone.utc) - latest.astimezone(timezone.utc)
        print(f"Dernière fenêtre Gold : {latest}; retard : {delay}")
        if delay > timedelta(minutes=15):
            raise RuntimeError(f"KPI obsolètes depuis {delay}")
        print("[SUCCESS] KPI récents")

    services = check_services()
    processes = check_streaming_processes()
    sync = sync_gold_to_timescaledb()
    freshness = check_data_freshness()
    [services, processes] >> sync >> freshness


industrypulse_streaming_pipeline()
