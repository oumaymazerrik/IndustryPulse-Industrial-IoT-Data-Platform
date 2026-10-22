
import json
import os
import time

import paho.mqtt.client as mqtt
from kafka import KafkaProducer


# ============================================================
# CONFIGURATION MQTT
# ============================================================

MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "industrypulse/machines")


# ============================================================
# CONFIGURATION KAFKA
# ============================================================

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "machine-sensors")


# ============================================================
# KAFKA PRODUCER
# ============================================================

def create_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                key_serializer=lambda key: key.encode("utf-8"),
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                retries=5,
                acks="all",
            )

            print("[OK] Kafka connecté", flush=True)
            return producer

        except Exception as error:
            print(f"[WAIT] Kafka indisponible : {error}", flush=True)
            time.sleep(5)


# ============================================================
# MQTT CALLBACKS
# ============================================================

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("[OK] Connexion MQTT réussie", flush=True)
        client.subscribe(MQTT_TOPIC, qos=1)
        print(f"[OK] Abonné à {MQTT_TOPIC}", flush=True)
    else:
        print(f"[ERROR] Connexion MQTT : {reason_code}", flush=True)


def on_message(client, userdata, message):
    try:
        data = json.loads(message.payload.decode("utf-8"))
        machine_id = data["machine_id"]

        future = producer.send(
            KAFKA_TOPIC,
            key=machine_id,
            value=data,
        )

        metadata = future.get(timeout=10)

        print(
            f"[MQTT -> KAFKA] "
            f"{machine_id} | "
            f"partition={metadata.partition} | "
            f"offset={metadata.offset}",
            flush=True,
        )

    except Exception as error:
        print(f"[ERROR] {error}", flush=True)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print("==========================================")
    print(" IndustryPulse - MQTT -> Kafka Bridge")
    print("==========================================")
    print(f"MQTT  : {MQTT_BROKER}:{MQTT_PORT}")
    print(f"Kafka : {KAFKA_BROKER}")
    print(f"Topic : {KAFKA_TOPIC}")
    print("==========================================")

    producer = create_kafka_producer()

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="industrypulse-mqtt-kafka-bridge",
    )

    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect_async(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_forever(retry_first_connection=True)

    except KeyboardInterrupt:
        print("\nArrêt du bridge...")

    finally:
        producer.flush()
        producer.close()
        client.disconnect()
        print("Connexions fermées.")
