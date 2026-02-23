# Payment Analytics Pipeline

End-to-end data pipeline for payment transaction analytics. Generates synthetic payment events, streams them through Kafka into PostgreSQL via PySpark Structured Streaming, runs daily batch transformations, and visualizes everything in Grafana - all orchestrated by Airflow.

## How it works

**Streaming:** A Python generator produces payment events that follow the real transaction lifecycle (authorization -> capture -> settlement, plus refunds and chargebacks). Events flow through Kafka, get validated and currency-normalized by PySpark Structured Streaming, and land in PostgreSQL.

**Batch:** Four PySpark jobs run daily: merchant net settlement, authorization rates by payment method, chargeback ratio monitoring against card scheme thresholds, and a conversion funnel. A data quality framework (8 check types) validates everything before it reaches the dashboard.

## Stack

- Apache Kafka (Confluent 7.5.3) - event streaming
- Apache Spark 3.5.3 / PySpark - streaming and batch processing
- PostgreSQL 16 - raw and analytics storage
- Apache Airflow 2.9.3 - orchestration
- Grafana 11.1.0 - dashboards
- Docker Compose - local deployment

## Getting started

```bash
git clone <[repo-url]>
cd payment-analytics-pipeline
docker-compose up -d
```

Wait a couple minutes for everything to come up, then:

- **Grafana** at http://localhost:3000 (admin / admin)
- **Airflow** at http://localhost:8081 (airflow / airflow)
- **Kafka UI** at http://localhost:8080
- **Spark UI** at http://localhost:4040

To run batch jobs:

```bash
make run-all-batch
```

Or trigger the Airflow DAG:

```bash
make airflow-trigger
```

Run `make help` for all available commands.
