
import json
import os
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

HOST = "0.0.0.0"
PORT = 8090

TOKEN = os.environ.get("SPARK_RUNNER_TOKEN", "")

SPARK_PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "org.postgresql:postgresql:42.7.7",
])

ALLOWED_JOBS = {
    # Pipeline Batch existant
    "bronze_silver": "/app/spark/iceberg_batch.py",
    "gold": "/app/spark/spark_batch_gold.py",
    "export_timescaledb": "/app/spark/spark_batch_to_timescaledb.py",

    # Synchronisation Streaming Gold -> TimescaleDB
    "gold_sync": "/app/spark/spark_gold_to_timescaledb.py",
}

job_lock = threading.Lock()


class SparkRunnerHandler(BaseHTTPRequestHandler):

    def send_json(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self.send_json(200, {"status": "healthy"})
        else:
            self.send_json(404, {"error": "Not found"})

    def do_POST(self):
        if self.path != "/run":
            self.send_json(404, {"error": "Not found"})
            return

        if not TOKEN or self.headers.get("X-Runner-Token") != TOKEN:
            self.send_json(401, {"error": "Unauthorized"})
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 4096:
                self.send_json(400, {"error": "Invalid request size"})
                return

            data = json.loads(self.rfile.read(length))
            job = data.get("job")
        except (ValueError, TypeError):
            self.send_json(400, {"error": "Invalid JSON"})
            return

        if job not in ALLOWED_JOBS:
            self.send_json(400, {"error": "Unknown job"})
            return

        if not job_lock.acquire(blocking=False):
            self.send_json(409, {"error": "Spark job already running"})
            return

        try:
            command = [
                "spark-submit",
                "--master", "local[2]",
                "--driver-memory", "2g",
                "--packages", SPARK_PACKAGES,
                ALLOWED_JOBS[job],
            ]

            print(f"Starting job: {job}", flush=True)

            try:
                result = subprocess.run(
                    command,
                    cwd="/app",
                    capture_output=True,
                    text=True,
                    timeout=1800,
                    check=False,
                )

                output = (
                    result.stdout + "\n" + result.stderr
                )[-12000:]

                print(
                    f"Job {job} finished with code {result.returncode}",
                    flush=True,
                )

                self.send_json(
                    200 if result.returncode == 0 else 500,
                    {
                        "job": job,
                        "returncode": result.returncode,
                        "output": output,
                    },
                )

            except subprocess.TimeoutExpired:
                self.send_json(
                    504,
                    {
                        "job": job,
                        "error": "Spark execution timeout",
                    },
                )

        finally:
            job_lock.release()


if __name__ == "__main__":
    print(
        f"Spark Runner listening on {HOST}:{PORT}",
        flush=True,
    )

    HTTPServer(
        (HOST, PORT),
        SparkRunnerHandler,
    ).serve_forever()
