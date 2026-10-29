
import os
import json
from pathlib import Path

import numpy as np
import pandas as pd
from kafka import KafkaConsumer, TopicPartition


REFERENCE_PATH = Path(
    os.getenv("REFERENCE_PATH", "/app/data/raw/ai4i2020.csv")
)

REPORT_PATH = Path(
    os.getenv("REPORT_PATH", "/app/reports/data_drift.json")
)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "machine-sensors")

MAX_MESSAGES = 1000
MIN_MESSAGES = 100

FEATURES = {
    "air_temperature": "Air temperature [K]",
    "process_temperature": "Process temperature [K]",
    "rotational_speed": "Rotational speed [rpm]",
    "torque": "Torque [Nm]",
    "tool_wear": "Tool wear [min]",
}


def calculate_psi(reference, current, bins=10):
    reference = np.asarray(reference, dtype=float)
    current = np.asarray(current, dtype=float)

    reference = reference[np.isfinite(reference)]
    current = current[np.isfinite(current)]

    if len(reference) == 0 or len(current) == 0:
        return None

    edges = np.unique(
        np.quantile(reference, np.linspace(0, 1, bins + 1))
    )

    if len(edges) < 2:
        return None

    edges[0] = -np.inf
    edges[-1] = np.inf

    ref_counts = np.histogram(reference, bins=edges)[0]
    cur_counts = np.histogram(current, bins=edges)[0]

    ref_pct = np.maximum(ref_counts / len(reference), 0.0001)
    cur_pct = np.maximum(cur_counts / len(current), 0.0001)

    return float(
        np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct))
    )


def read_recent_kafka():
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BROKER,
        enable_auto_commit=False,
        consumer_timeout_ms=3000,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    records = []

    try:
        partitions = consumer.partitions_for_topic(KAFKA_TOPIC)

        if not partitions:
            raise RuntimeError("Kafka topic not found")

        topic_partitions = [
            TopicPartition(KAFKA_TOPIC, p)
            for p in sorted(partitions)
        ]

        consumer.assign(topic_partitions)

        end_offsets = consumer.end_offsets(topic_partitions)
        beginning_offsets = consumer.beginning_offsets(topic_partitions)

        remaining = MAX_MESSAGES

        for tp in topic_partitions:
            available = end_offsets[tp] - beginning_offsets[tp]
            count = min(available, remaining)

            consumer.seek(tp, end_offsets[tp] - count)
            remaining -= count

            if remaining <= 0:
                break

        # Read a bounded snapshot without joining a consumer group.
        target_offsets = end_offsets.copy()

        while len(records) < MAX_MESSAGES:
            batches = consumer.poll(timeout_ms=1000)

            if not batches:
                break

            for tp, messages in batches.items():
                for message in messages:
                    if message.offset >= target_offsets[tp]:
                        continue

                    data = message.value

                    if all(
                        isinstance(data.get(feature), (int, float))
                        and not isinstance(data.get(feature), bool)
                        and np.isfinite(data[feature])
                        for feature in FEATURES
                    ):
                        records.append(data)

                    if len(records) >= MAX_MESSAGES:
                        break

                if len(records) >= MAX_MESSAGES:
                    break

        return pd.DataFrame(records)

    finally:
        consumer.close()


def main():
    reference = pd.read_csv(REFERENCE_PATH)
    current = read_recent_kafka()

    if len(current) < MIN_MESSAGES:
        raise RuntimeError(
            f"Not enough IoT data: {len(current)} "
            f"(minimum {MIN_MESSAGES})"
        )

    results = {}

    for feature, reference_column in FEATURES.items():
        psi = calculate_psi(
            reference[reference_column].dropna(),
            current[feature].dropna(),
        )

        if psi is None:
            status = "INSUFFICIENT_VARIATION"
        elif psi < 0.1:
            status = "STABLE"
        elif psi < 0.2:
            status = "MODERATE_DRIFT"
        else:
            status = "HIGH_DRIFT"

        results[feature] = {
            "psi": round(psi, 4) if psi is not None else None,
            "status": status,
        }

    report = {
        "reference_rows": len(reference),
        "current_rows": len(current),
        "method": "PSI",
        "features": results,
    }

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(REPORT_PATH, "w", encoding="utf-8") as file:
        json.dump(report, file, indent=2)

    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
