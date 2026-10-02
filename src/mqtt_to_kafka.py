import json

import paho.mqtt.client as mqtt
from kafka import KafkaProducer


# ============================================================
# CONFIGURATION MQTT
# ============================================================

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "industrypulse/machines"


# ============================================================
# CONFIGURATION KAFKA
# ============================================================

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "machine-sensors"


# ============================================================
# KAFKA PRODUCER
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,

    key_serializer=lambda key: key.encode("utf-8"),

    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# ============================================================
# MQTT CALLBACK
# ============================================================

def on_connect(client, userdata, flags, reason_code, properties):

    if reason_code == 0:

        print("Connexion MQTT réussie")

        client.subscribe(
            MQTT_TOPIC,
            qos=1
        )

        print(f"Abonné au topic : {MQTT_TOPIC}")

    else:

        print(f"Erreur MQTT : {reason_code}")


def on_message(client, userdata, message):

    try:

        # Message MQTT -> JSON
        data = json.loads(
            message.payload.decode("utf-8")
        )

        machine_id = data["machine_id"]

        # JSON -> Kafka
        future = producer.send(
            KAFKA_TOPIC,
            key=machine_id,
            value=data
        )

        metadata = future.get(timeout=10)

        print(
            f"[MQTT -> KAFKA] "
            f"{machine_id} | "
            f"partition={metadata.partition} | "
            f"offset={metadata.offset}"
        )

    except Exception as error:

        print(f"[ERROR] {error}")


# ============================================================
# MQTT CLIENT
# ============================================================

client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="industrypulse-mqtt-kafka-bridge"
)

client.on_connect = on_connect
client.on_message = on_message


# ============================================================
# START
# ============================================================

print("==========================================")
print(" IndustryPulse - MQTT -> Kafka Bridge")
print("==========================================")
print(f"MQTT  : {MQTT_BROKER}:{MQTT_PORT}")
print(f"Kafka : {KAFKA_BROKER}")
print(f"Topic : {KAFKA_TOPIC}")
print("==========================================")

client.connect(
    MQTT_BROKER,
    MQTT_PORT,
    60
)

try:

    client.loop_forever()

except KeyboardInterrupt:

    print("\nArrêt du bridge...")

finally:

    producer.flush()
    producer.close()
    client.disconnect()

    print("Connexions fermées.")