
import os
import json
import urllib.request
import urllib.error

from datetime import datetime, timedelta

from airflow.sdk import dag, task


SPARK_RUNNER_URL = "http://spark:8090/run"


@dag(
    dag_id="industrypulse_gold_sync",
    description="Synchronisation automatique Iceberg Gold vers TimescaleDB",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=[
        "industrypulse",
        "gold",
        "timescaledb",
        "streaming",
    ],
)
def industrypulse_gold_sync():

    @task
    def sync_gold_to_timescaledb():

        token = os.environ["SPARK_RUNNER_TOKEN"]

        payload = json.dumps({
            "job": "gold_sync"
        }).encode("utf-8")

        request = urllib.request.Request(
            SPARK_RUNNER_URL,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "X-Runner-Token": token,
            },
            method="POST",
        )

        print("[INFO] Demarrage Gold Sync...")

        try:
            with urllib.request.urlopen(
                request,
                timeout=1500,
            ) as response:

                result = json.loads(
                    response.read().decode("utf-8")
                )

        except urllib.error.HTTPError as error:

            details = error.read().decode("utf-8")

            raise RuntimeError(
                f"Spark Runner HTTP {error.code}: "
                f"{details}"
            ) from error

        if result.get("returncode") != 0:

            raise RuntimeError(
                f"Gold Sync a echoue : {result}"
            )

        print("[SUCCESS] Gold Sync termine.")
        print(result.get("output", ""))

    sync_gold_to_timescaledb()


industrypulse_gold_sync()
