# IndustryPulse — Industrial IoT Data Platform

> End-to-end Big Data platform for industrial IoT data ingestion, real-time processing, Lakehouse storage, analytics, and predictive maintenance.

IndustryPulse is an end-to-end **Industrial IoT Data Platform** designed to collect, process, store, and analyze industrial machine data using modern **Big Data and Data Engineering technologies**.

The platform combines **real-time IoT telemetry** with **historical predictive-maintenance data** through batch and streaming pipelines. It follows a **Medallion Lakehouse Architecture (Bronze → Silver → Gold)** built with Apache Spark, Apache Iceberg, and MinIO.

The project is designed to progressively integrate a serving layer, analytics, monitoring, orchestration, machine learning, and MLOps capabilities.

---

## Project Objectives

IndustryPulse aims to simulate a realistic industrial data platform capable of:

- Collecting machine telemetry in real time
- Ingesting IoT events through MQTT
- Streaming events through Apache Kafka
- Processing real-time data with Spark Structured Streaming
- Processing historical industrial datasets with Spark Batch
- Applying data quality and validation rules
- Building Bronze, Silver, and Gold data layers
- Computing industrial KPIs in near real time
- Storing analytical datasets using Apache Iceberg
- Using MinIO as S3-compatible object storage
- Preparing data for dashboards and business analytics
- Supporting predictive-maintenance machine learning models
- Providing a foundation for orchestration and MLOps

---

## Architecture

```text
                         INDUSTRYPULSE
               Industrial IoT Data Platform

     ┌──────────────────────┐
     │   Historical Data    │
     │      AI4I 2020       │
     └──────────┬───────────┘
                │
                ▼
          Apache Spark Batch
                │
                ▼
        ┌───────────────────┐
        │   Bronze Layer    │
        │     ai4i_raw      │
        └─────────┬─────────┘
                  │
            Cleaning /
          Transformation
                  │
                  ▼
        ┌───────────────────┐
        │   Silver Layer    │
        │    ai4i_clean     │
        └───────────────────┘


     ┌──────────────────────┐
     │ IoT Sensor Generator │
     │   Industrial Machines│
     └──────────┬───────────┘
                │
                ▼
              MQTT
          Eclipse Mosquitto
                │
                ▼
         MQTT → Kafka Bridge
                │
                ▼
          Apache Kafka
                │
                ▼
     Spark Structured Streaming
                │
                ▼
        ┌──────────────────────┐
        │     Bronze Layer     │
        │ machine_sensors_raw  │
        └──────────┬───────────┘
                   │
             Data Quality
              Validation
             Enrichment
                   │
                   ▼
        ┌──────────────────────┐
        │     Silver Layer     │
        │machine_sensors_clean │
        └──────────┬───────────┘
                   │
            Windowed KPI
            Aggregations
                   │
                   ▼
        ┌──────────────────────┐
        │      Gold Layer      │
        │     machine_kpi      │
        └──────────┬───────────┘
                   │
                   ▼
          Apache Iceberg
                   │
                   ▼
                MinIO
          S3 Object Storage
```

### Target Architecture

The platform is progressively evolving toward:

```text
IoT / Historical Data
        │
        ▼
 MQTT + Kafka
        │
        ▼
 Apache Spark
 Batch + Streaming
        │
        ▼
 MinIO + Iceberg
 Bronze / Silver / Gold
        │
        ├──────────────► PostgreSQL / TimescaleDB
        │                        │
        │                  ┌─────┴─────┐
        │                  ▼           ▼
        │               Grafana     Power BI
        │
        ├──────────────► dbt Analytics
        │
        └──────────────► Machine Learning
                          │
                          ▼
                   Predictive Maintenance
                          │
                          ▼
                        MLflow

                 Apache Airflow
              Pipeline Orchestration
```

---

## Technology Stack

| Category              | Technologies                                 |
| --------------------- | -------------------------------------------- |
| IoT Messaging         | MQTT, Eclipse Mosquitto                      |
| Event Streaming       | Apache Kafka                                 |
| Data Processing       | Apache Spark 3.5, Spark Structured Streaming |
| Programming           | Python                                       |
| Lakehouse             | Apache Iceberg                               |
| Object Storage        | MinIO                                        |
| Architecture          | Medallion Architecture                       |
| Data Processing Modes | Batch + Streaming                            |
| Containerization      | Docker, Docker Compose                       |
| Serving Layer         | PostgreSQL / TimescaleDB _(planned)_         |
| Transformation        | dbt _(planned)_                              |
| Orchestration         | Apache Airflow _(planned)_                   |
| Monitoring            | Grafana _(planned)_                          |
| Business Intelligence | Power BI _(planned)_                         |
| Machine Learning      | Predictive Maintenance _(planned)_           |
| MLOps                 | MLflow _(planned)_                           |

---

## Medallion Architecture

IndustryPulse implements the **Bronze → Silver → Gold** architecture to progressively improve data quality and business value.

### Bronze Layer

The Bronze layer stores raw or minimally transformed data.

Current Iceberg tables:

```text
industrypulse.bronze.ai4i_raw
industrypulse.bronze.machine_sensors_raw
```

For streaming data, Kafka metadata is preserved, including:

- Kafka partition
- Kafka offset
- Kafka timestamp
- Ingestion timestamp

This provides traceability between the messaging layer and the Lakehouse.

### Silver Layer

The Silver layer contains validated, cleaned, and enriched datasets.

Current tables:

```text
industrypulse.silver.ai4i_clean
industrypulse.silver.machine_sensors_clean
```

The real-time pipeline applies data-quality rules to fields such as:

- Machine identifier
- Event timestamp
- Temperature
- Vibration
- Pressure
- Energy consumption

It also generates additional features:

```text
temperature_alert
vibration_alert
sensor_alert
data_quality_status
processing_timestamp
```

### Gold Layer

The Gold layer provides business-oriented industrial KPIs.

Current table:

```text
industrypulse.gold.machine_kpi
```

KPIs are calculated for each machine using **one-minute event-time windows**.

Examples include:

- Total measurements
- Average temperature
- Minimum temperature
- Maximum temperature
- Average vibration
- Maximum vibration
- Average pressure
- Average energy consumption
- Number of alerts
- Alert rate

These datasets are designed to feed dashboards, monitoring systems, and downstream analytics.

---

## Real-Time Pipeline

The real-time pipeline simulates telemetry generated by industrial machines.

```text
Machine Simulator
      │
      ▼
     MQTT
      │
      ▼
MQTT → Kafka Bridge
      │
      ▼
    Kafka
      │
      ▼
Spark Structured Streaming
      │
      ▼
Bronze Iceberg
      │
      ▼
Data Quality + Enrichment
      │
      ▼
Silver Iceberg
      │
      ▼
KPI Aggregation
      │
      ▼
Gold Iceberg
```

The IoT simulator currently generates telemetry for five machines:

```text
M001
M002
M003
M004
M005
```

Each event contains information such as:

```json
{
  "machine_id": "M001",
  "temperature": 72.45,
  "vibration": 0.532,
  "pressure": 5.82,
  "energy": 43.21,
  "status": "RUNNING",
  "timestamp": "2026-10-04T14:06:25Z"
}
```

---

## Historical Batch Pipeline

IndustryPulse also contains a batch-processing pipeline based on the **AI4I 2020 Predictive Maintenance Dataset**.

The dataset contains industrial-machine information such as:

- Air temperature
- Process temperature
- Rotational speed
- Torque
- Tool wear
- Machine failure
- Tool wear failure
- Heat dissipation failure
- Power failure
- Overstrain failure
- Random failure

Spark performs transformations including:

- Duplicate removal
- Column standardization
- Data validation
- Temperature conversion
- Temperature-difference calculation
- Mechanical-power calculation
- Industrial risk enrichment

The resulting data is stored in Iceberg Bronze and Silver tables.

---

## Data Quality

Data quality is integrated directly into the streaming pipeline.

Examples of validation rules:

```text
machine_id       → required
event_timestamp  → required

0 ≤ temperature ≤ 150
0 ≤ vibration   ≤ 5
0 ≤ pressure    ≤ 20
energy          ≥ 0
```

Invalid observations are identified through a `data_quality_status` field.

Alert rules currently include:

```text
Temperature > 85°C  → Temperature Alert

Vibration > 1.0     → Vibration Alert

Temperature Alert
        OR
Vibration Alert     → Sensor Alert
```

These thresholds are currently used as **simulation rules for the project** and should not be interpreted as universal industrial safety limits.

---

## Lakehouse Storage

IndustryPulse uses:

- **MinIO** for S3-compatible object storage
- **Apache Iceberg** as the analytical table format

The Iceberg warehouse is stored in:

```text
s3a://industrypulse/warehouse
```

The current logical organization is:

```text
industrypulse
│
├── bronze
│   ├── ai4i_raw
│   └── machine_sensors_raw
│
├── silver
│   ├── ai4i_clean
│   └── machine_sensors_clean
│
└── gold
    └── machine_kpi
```

Iceberg provides a modern table abstraction on top of object storage and prepares the platform for scalable analytical workloads.

---

## Project Structure

```text
IndustryPulse/
│
├── data/
│   └── raw/
│       └── ai4i2020.csv
│
├── mosquitto/
│   ├── config/
│   │   └── mosquitto.conf
│   ├── data/
│   └── log/
│
├── spark/
│   ├── spark_batch.py
│   ├── spark_streaming.py
│   ├── iceberg_batch.py
│   ├── spark_streaming_iceberg.py
│   ├── spark_silver_streaming.py
│   ├── spark_gold_streaming.py
│   └── test_minio.py
│
├── src/
│   ├── iot_generator.py
│   └── mqtt_to_kafka.py
│
├── docker-compose.yml
├── .gitignore
└── README.md
```

> The exact structure may evolve as new components such as Airflow, dbt, MLflow, Grafana, and the serving layer are integrated.

---

## Getting Started

### Prerequisites

Install:

- Docker Desktop
- Python 3
- Git

Python dependencies currently include:

```bash
pip install paho-mqtt kafka-python
```

---

### 1. Clone the Repository

```bash
git clone <repository-url>
cd IndustryPulse-Industrial-IoT-Data-Platform
```

---

### 2. Start the Infrastructure

```bash
docker compose up -d
```

Verify the containers:

```bash
docker ps
```

The current infrastructure includes:

```text
Eclipse Mosquitto
Apache Kafka
Apache Spark
MinIO
```

---

### 3. Start the IoT Generator

```bash
python src/iot_generator.py
```

---

### 4. Start the MQTT → Kafka Bridge

In another terminal:

```bash
python src/mqtt_to_kafka.py
```

---

### 5. Start Bronze Streaming

```bash
docker exec -it industrypulse-spark spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /app/spark/spark_streaming_iceberg.py
```

---

### 6. Start Silver Streaming

```bash
docker exec -it industrypulse-spark spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /app/spark/spark_silver_streaming.py
```

---

### 7. Start Gold KPI Processing

```bash
docker exec -it industrypulse-spark spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /app/spark/spark_gold_streaming.py
```

After the pipelines start, data continuously flows through:

```text
MQTT
  ↓
Kafka
  ↓
Bronze
  ↓
Silver
  ↓
Gold
```

---

## Example Gold Analytics

The Gold layer can be queried to analyze machine behavior.

```sql
SELECT
    machine_id,
    window_start,
    window_end,
    total_measurements,
    avg_temperature,
    max_temperature,
    avg_vibration,
    max_vibration,
    avg_energy,
    total_alerts,
    alert_rate
FROM industrypulse.gold.machine_kpi
ORDER BY window_start DESC, machine_id;
```

Example aggregated analysis:

```sql
SELECT
    machine_id,
    ROUND(AVG(avg_temperature), 2) AS avg_temperature,
    ROUND(MAX(max_temperature), 2) AS max_temperature,
    ROUND(AVG(avg_vibration), 3) AS avg_vibration,
    SUM(total_alerts) AS total_alerts,
    ROUND(AVG(alert_rate), 2) AS avg_alert_rate
FROM industrypulse.gold.machine_kpi
GROUP BY machine_id
ORDER BY total_alerts DESC;
```

---

## Current Project Status

| Component                  | Status |
| -------------------------- | :----: |
| IoT Machine Simulator      |   ✅   |
| MQTT / Mosquitto           |   ✅   |
| MQTT → Kafka Bridge        |   ✅   |
| Apache Kafka               |   ✅   |
| Spark Batch                |   ✅   |
| Spark Structured Streaming |   ✅   |
| MinIO                      |   ✅   |
| Apache Iceberg             |   ✅   |
| Historical Bronze Layer    |   ✅   |
| Historical Silver Layer    |   ✅   |
| Streaming Bronze Layer     |   ✅   |
| Streaming Silver Layer     |   ✅   |
| Streaming Data Quality     |   ✅   |
| Streaming Gold KPI Layer   |   ✅   |
| PostgreSQL / TimescaleDB   |   🔜   |
| dbt Analytics              |   🔜   |
| Grafana Dashboard          |   🔜   |
| Power BI Dashboard         |   🔜   |
| Airflow Orchestration      |   🔜   |
| Predictive Maintenance ML  |   🔜   |
| MLflow                     |   🔜   |

---

## Roadmap

The next development phases include:

1. **Serving Layer**
   - PostgreSQL
   - TimescaleDB
   - Gold KPI synchronization

2. **Industrial Monitoring**
   - Grafana dashboards
   - Machine health monitoring
   - Temperature and vibration monitoring
   - Alert visualization

3. **Business Analytics**
   - Power BI dashboards
   - Historical machine analysis
   - Failure analysis
   - Energy-consumption analytics

4. **Analytics Engineering**
   - dbt models
   - Business transformations
   - Data tests and documentation

5. **Predictive Maintenance**
   - Failure prediction
   - Feature engineering
   - Model training and evaluation

6. **MLOps**
   - MLflow experiment tracking
   - Model registry
   - Model versioning

7. **Orchestration**
   - Apache Airflow DAGs
   - Automated batch processing
   - Data-quality workflows
   - ML pipeline orchestration

---

## Engineering Concepts Demonstrated

This project demonstrates practical experience with:

- Data Engineering
- Industrial IoT
- Event-Driven Architecture
- Batch Processing
- Stream Processing
- Apache Kafka
- Spark Structured Streaming
- Data Lakehouse Architecture
- Apache Iceberg
- Object Storage
- Medallion Architecture
- Data Quality
- Event-Time Processing
- Windowed Aggregations
- Industrial KPI Engineering
- Dockerized Data Infrastructure

---

## Security Notice

The current configuration is intended for **local development and educational purposes**.

Some services use simplified development settings such as anonymous MQTT access and local credentials.

For a production deployment, the platform should include:

- MQTT authentication
- TLS encryption
- Kafka authentication and authorization
- Secret management
- MinIO access policies
- Network isolation
- Role-based access control
- Secure credential management

Secrets and environment-specific credentials should never be committed to Git.

---

## Author

**Oumayma Zerrik**

Master's Student — Big Data & IoT  
Data Engineering • Big Data • AI • MLOps

---

## Project Status

🚧 **Active Development**

IndustryPulse is being developed incrementally as an end-to-end industrial data engineering platform. New components and architectural improvements will be added progressively.
