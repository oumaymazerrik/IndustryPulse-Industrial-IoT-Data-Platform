import json
import random
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt


# ============================================================
# CONFIGURATION MQTT
# ============================================================

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "industrypulse/machines"


# ============================================================
# MACHINES
# ============================================================

MACHINES = [
    "M001",
    "M002",
    "M003",
    "M004",
    "M005"
]


# ============================================================
# CLIENT MQTT
# ============================================================

client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="industrypulse-iot-generator"
)

client.connect(MQTT_BROKER, MQTT_PORT, 60)

client.loop_start()

print("========================================")
print(" IndustryPulse - IoT Generator")
print("========================================")
print(f"MQTT Broker : {MQTT_BROKER}:{MQTT_PORT}")
print(f"Topic       : {MQTT_TOPIC}")
print(f"Machines    : {len(MACHINES)}")
print("========================================\n")


# ============================================================
# GENERATION DES DONNEES
# ============================================================

def generate_sensor_data(machine_id):

    temperature = round(random.uniform(55, 90), 2)
    vibration = round(random.uniform(0.1, 1.2), 3)
    pressure = round(random.uniform(4.0, 7.0), 2)
    energy = round(random.uniform(20, 80), 2)

    # Détection simple d'un état anormal
    if temperature > 85 or vibration > 1.0:
        status = "WARNING"
    else:
        status = "RUNNING"

    return {
        "machine_id": machine_id,
        "temperature": temperature,
        "vibration": vibration,
        "pressure": pressure,
        "energy": energy,
        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


# ============================================================
# SIMULATION
# ============================================================

try:

    while True:

        for machine in MACHINES:

            data = generate_sensor_data(machine)

            message = json.dumps(data)

            client.publish(
                MQTT_TOPIC,
                message,
                qos=1
            )

            print(
                f"[SENT] {machine} | "
                f"Temp={data['temperature']} C | "
                f"Vibration={data['vibration']} | "
                f"Pressure={data['pressure']} | "
                f"Energy={data['energy']} | "
                f"Status={data['status']}"
            )

        print("-" * 70)

        time.sleep(5)


except KeyboardInterrupt:

    print("\nArrêt du générateur IoT...")

finally:

    client.loop_stop()
    client.disconnect()

    print("Connexion MQTT fermée.")