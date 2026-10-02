# IndustryPulse - Industrial IoT Data Platform

IndustryPulse est une plateforme de donnees IoT industrielle qui simule des capteurs de machines, publie les mesures via MQTT, puis les transfere vers Apache Kafka pour alimenter des pipelines de traitement temps reel.

## Architecture

```text
IoT Generator -> MQTT Mosquitto -> MQTT to Kafka Bridge -> Apache Kafka
```

## Contenu du projet

- `src/iot_generator.py` : genere des donnees de capteurs pour plusieurs machines industrielles.
- `src/mqtt_to_kafka.py` : consomme les messages MQTT et les publie dans Kafka.
- `docker-compose.yml` : lance Mosquitto et Kafka en local.
- `mosquitto/config/mosquitto.conf` : configuration du broker MQTT.

## Prerequis

- Docker et Docker Compose
- Python 3.10+
- Dependances Python :

```bash
pip install paho-mqtt kafka-python
```

## Lancement

1. Demarrer l'infrastructure :

```bash
docker compose up -d
```

2. Lancer le bridge MQTT vers Kafka :

```bash
python src/mqtt_to_kafka.py
```

3. Dans un autre terminal, lancer le generateur IoT :

```bash
python src/iot_generator.py
```

## Topics

- Topic MQTT : `industrypulse/machines`
- Topic Kafka : `machine-sensors`

## Exemple de message

```json
{
  "machine_id": "M001",
  "temperature": 72.45,
  "vibration": 0.482,
  "pressure": 5.31,
  "energy": 46.7,
  "status": "RUNNING",
  "timestamp": "2026-10-02T11:00:00+00:00"
}
```

## Arret

Arreter les services Docker :

```bash
docker compose down
```
