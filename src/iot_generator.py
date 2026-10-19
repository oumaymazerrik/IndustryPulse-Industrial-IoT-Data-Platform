
import json
import os
import random
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt


# ============================================================
# CONFIGURATION MQTT
# ============================================================

MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "industrypulse/machines")

GENERATION_INTERVAL = float(
    os.getenv("GENERATION_INTERVAL", "5")
)


# ============================================================
# MACHINES
# ============================================================

MACHINES = [
    "M001",
    "M002",
    "M003",
    "M004",
    "M005",
]

# Usure propre a chaque machine
machine_tool_wear = {
    machine: random.randint(0, 100)
    for machine in MACHINES
}


# ============================================================
# GENERATION DES DONNEES
# ============================================================

def generate_sensor_data(machine_id):

    # Capteurs IoT existants
    temperature = round(random.uniform(55, 90), 2)
    vibration = round(random.uniform(0.1, 1.2), 3)
    pressure = round(random.uniform(4.0, 7.0), 2)
    energy = round(random.uniform(20, 80), 2)

    # Variables synthetiques compatibles avec AI4I
    # Temperatures en Kelvin
    air_temperature = round(random.uniform(295, 305), 2)

    process_temperature = round(
        air_temperature + random.uniform(8, 12), 2
    )

    # Vitesse en RPM
    rotational_speed = random.randint(1200, 2200)

    # Couple moteur en Nm
    torque = round(
        max(
            5.0,
            min(
                80.0,
                45 - 0.035 * (rotational_speed - 1500)
                + random.uniform(-8, 8)
            )
        ),
        2
    )

    # Usure en minutes
    tool_wear = machine_tool_wear[machine_id]

    machine_tool_wear[machine_id] += random.choice([0, 1, 2])

    if machine_tool_wear[machine_id] > 250:
        machine_tool_wear[machine_id] = 0

    # Alertes IoT existantes
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

        # Variables pour XGBoost
        "air_temperature": air_temperature,
        "process_temperature": process_temperature,
        "rotational_speed": rotational_speed,
        "torque": torque,
        "tool_wear": tool_wear,

        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ============================================================
# MQTT CONNECTION
# ============================================================

mqtt_connected = False


def on_connect(client, userdata, flags, reason_code, properties):
    global mqtt_connected

    mqtt_connected = reason_code == 0

    if mqtt_connected:
        print("[OK] Connexion MQTT reussie", flush=True)
    else:
        print(
            f"[ERROR] Connexion MQTT : {reason_code}",
            flush=True,
        )


def on_disconnect(
    client,
    userdata,
    disconnect_flags,
    reason_code,
    properties
):
    global mqtt_connected

    mqtt_connected = False

    print(
        f"[WARNING] MQTT deconnecte : {reason_code}",
        flush=True,
    )


# ============================================================
# MAIN
# ============================================================

def main():

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="industrypulse-iot-generator",
    )

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    client.reconnect_delay_set(
        min_delay=1,
        max_delay=30,
    )

    print("========================================")
    print(" IndustryPulse - IoT Generator + ML")
    print("========================================")
    print(f"MQTT Broker : {MQTT_BROKER}:{MQTT_PORT}")
    print(f"Topic       : {MQTT_TOPIC}")
    print(f"Machines    : {len(MACHINES)}")
    print(f"Intervalle  : {GENERATION_INTERVAL} s")
    print("========================================")

    try:
        client.connect_async(
            MQTT_BROKER,
            MQTT_PORT,
            60,
        )

        client.loop_start()

        while True:

            if not mqtt_connected:
                print(
                    "[WAIT] Attente de la connexion MQTT...",
                    flush=True,
                )
                time.sleep(2)
                continue

            for machine in MACHINES:

                data = generate_sensor_data(machine)

                message = json.dumps(data)

                result = client.publish(
                    MQTT_TOPIC,
                    message,
                    qos=1,
                )

                try:
                    result.wait_for_publish(timeout=10)

                    if not result.is_published():
                        raise RuntimeError(
                            "Publication MQTT non confirmee"
                        )

                    print(
                        f"[SENT] {machine} | "
                        f"Temp={data['temperature']} C | "
                        f"Vibration={data['vibration']} | "
                        f"Pressure={data['pressure']} | "
                        f"Energy={data['energy']} | "
                        f"AirTemp={data['air_temperature']} K | "
                        f"ProcessTemp={data['process_temperature']} K | "
                        f"RPM={data['rotational_speed']} | "
                        f"Torque={data['torque']} Nm | "
                        f"Wear={data['tool_wear']} min | "
                        f"Status={data['status']}",
                        flush=True,
                    )

                except Exception as error:
                    print(
                        f"[ERROR] Publication MQTT : {error}",
                        flush=True,
                    )

            print("-" * 70, flush=True)

            time.sleep(GENERATION_INTERVAL)

    except KeyboardInterrupt:
        print("\nArret du generateur IoT...")

    finally:
        client.disconnect()
        client.loop_stop()

        print("Connexion MQTT fermee.")


if __name__ == "__main__":
    main()
