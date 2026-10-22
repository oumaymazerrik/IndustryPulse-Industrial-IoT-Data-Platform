
import json
import os
import time
import urllib.request
import urllib.error

import psycopg2
from kafka import KafkaConsumer


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "machine-sensors")
KAFKA_GROUP = "industrypulse-ml-inference-v1"

API_URL = os.getenv(
    "ML_API_URL",
    "http://industrypulse-ml-api:8000/predict"
)

DB_HOST = os.getenv("DB_HOST", "timescaledb")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "industrypulse")
DB_USER = os.getenv("DB_USER", "industrypulse")
DB_PASSWORD = os.getenv("DB_PASSWORD")

FEATURES = [
    "air_temperature",
    "process_temperature",
    "rotational_speed",
    "torque",
    "tool_wear",
]


def validate_data(data):
    if not isinstance(data, dict):
        return False

    if not data.get("machine_id") or not data.get("timestamp"):
        return False

    for feature in FEATURES:
        value = data.get(feature)

        if value is None:
            return False

        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return False

        if not float("-inf") < value < float("inf"):
            return False

    return (
        290 <= data["air_temperature"] <= 315
        and 290 <= data["process_temperature"] <= 330
        and 1000 <= data["rotational_speed"] <= 3000
        and 0 <= data["torque"] <= 100
        and 0 <= data["tool_wear"] <= 300
    )


def predict(data):
    payload = {
        feature: data[feature]
        for feature in FEATURES
    }

    request = urllib.request.Request(
        API_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def connect_db():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=10,
    )


def save_prediction(connection, message, data, result):
    query = """
        INSERT INTO public.machine_predictions (
            machine_id,
            event_timestamp,
            kafka_partition,
            kafka_offset,
            failure_probability,
            threshold,
            prediction,
            status,
            model
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (kafka_partition, kafka_offset)
        DO NOTHING
    """

    with connection.cursor() as cursor:
        cursor.execute(
            query,
            (
                data["machine_id"],
                data["timestamp"],
                message.partition,
                message.offset,
                result["failure_probability"],
                result["threshold"],
                result["prediction"],
                result["status"],
                result["model"],
            ),
        )

    connection.commit()


def main():
    if not DB_PASSWORD:
        raise RuntimeError("DB_PASSWORD is not configured")

    print("[INFO] Starting IndustryPulse ML Worker", flush=True)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=KAFKA_GROUP,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    print("[INFO] Connected to Kafka", flush=True)

    connection = None

    try:
        for message in consumer:
            data = message.value

            if not validate_data(data):
                print(
                    f"[SKIPPED] Invalid ML data at offset {message.offset}",
                    flush=True,
                )
                consumer.commit()
                continue

            while True:
                try:
                    if connection is None or connection.closed:
                        connection = connect_db()
                        print("[INFO] Connected to TimescaleDB", flush=True)

                    result = predict(data)

                    save_prediction(connection, message, data, result)

                    print(
                        f"[SAVED] Machine={data['machine_id']} "
                        f"Probability={result['failure_probability']} "
                        f"Status={result['status']} "
                        f"Offset={message.offset}",
                        flush=True,
                    )

                    consumer.commit()
                    break

                except (
                    psycopg2.Error,
                    urllib.error.URLError,
                    TimeoutError,
                    ValueError,
                    KeyError,
                ) as error:
                    print(
                        f"[ERROR] {error}. Retrying in 5 seconds...",
                        flush=True,
                    )

                    if connection is not None:
                        try:
                            connection.rollback()
                            connection.close()
                        except psycopg2.Error:
                            pass
                        connection = None

                    time.sleep(5)

    finally:
        if connection is not None:
            connection.close()
        consumer.close()


if __name__ == "__main__":
    main()
