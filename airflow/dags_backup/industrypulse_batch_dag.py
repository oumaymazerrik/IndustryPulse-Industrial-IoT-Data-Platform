
import json
import os
import urllib.error
import urllib.request

from datetime import datetime, timedelta

from airflow.sdk import dag, task


RUNNER_URL = "http://spark:8090/run"


def run_spark_job(job_name):
    token = os.environ["SPARK_RUNNER_TOKEN"]

    payload = json.dumps({
        "job": job_name
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

    try:
        with urllib.request.urlopen(request, timeout=1900) as response:
            result = json.loads(response.read().decode("utf-8"))

    except urllib.error.HTTPError as error:
        details = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Spark job {job_name} failed: {details[-12000:]}"
        ) from error

    if result.get("returncode") != 0:
        raise RuntimeError(
            f"Spark job {job_name} failed: {result}"
        )

    print(result.get("output", ""))
    print(f"Spark job completed: {job_name}")


@dag(
    dag_id="industrypulse_batch_pipeline",
    description="IndustryPulse Batch - Iceberg, Spark Gold, TimescaleDB",
    schedule=None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 0,
        "execution_timeout": timedelta(minutes=35),
    },
    tags=["industrypulse", "spark", "iceberg", "batch"],
)
def industrypulse_batch_pipeline():

    @task
    def bronze_silver():
        run_spark_job("bronze_silver")

    @task
    def gold():
        run_spark_job("gold")

    @task
    def export_timescaledb():
        run_spark_job("export_timescaledb")

    bronze_silver() >> gold() >> export_timescaledb()


industrypulse_batch_pipeline()
