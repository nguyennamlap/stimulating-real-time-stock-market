# Stimulating Real-Time Stock Market

A near real-time stock market data pipeline built with **Airflow, Kafka, Spark Structured Streaming, dbt, Postgres, and Docker**.  
The project ingests stock data, validates and cleans it, streams it through Kafka, enriches it with technical indicators in Spark, and transforms it into analytics-ready models with dbt.

---

## Overview

This repository demonstrates an end-to-end data engineering workflow for market data:

1. **Ingestion** — crawl stock history data from the source.
2. **Quality Control** — validate schemas and filter bad records.
3. **Streaming** — publish clean records to Kafka.
4. **Processing** — enrich data in Spark with technical indicators.
5. **Orchestration** — coordinate jobs with Airflow.
6. **Transformation** — build staging, intermediate, and mart models with dbt.
7. **Monitoring** — log each stage for easier debugging and operational visibility.

The result is a production-style pipeline that is suitable for analytics, feature engineering, and downstream trading research.



## Architecture

```text
Stock Source
   ↓
Ingestion / Crawl
   ↓
Validation & Cleaning
   ↓
Kafka Topic
   ↓
Spark Structured Streaming
   ↓
Postgres Raw Layer
   ↓
dbt Transformations
   ├── staging
   ├── intermediate
   └── marts
   ↓
Analytics / BI / Feature Tables
```

---

## Key Features

- Stock data crawling and incremental loading
- Schema validation with Pandera
- Kafka-based event streaming
- Spark Structured Streaming with watermarking
- Technical indicator enrichment:
  - EMA 20 / 50 / 200
  - RSI
  - MACD line, signal, histogram
  - Bollinger Bands
- dbt layer separation for clean analytics modeling
- Data quality tests:
  - `not_null_ticker`
  - `positive_price`
  - `unique_ticker_date`
- Airflow orchestration with alerting support
- Dockerized services for reproducible local development
- Centralized logging for ingestion, validation, Kafka, and Spark jobs

---

## Tech Stack

- **Orchestration:** Apache Airflow
- **Streaming:** Apache Kafka, Spark Structured Streaming
- **Storage:** PostgreSQL, MinIO
- **Transformation:** dbt
- **Validation:** Pandera, custom pandas-based checks
- **Extraction:** Requests, pandas, boto3
- **Containerization:** Docker, Docker Compose
- **Monitoring & Logging:** Custom Python logging

---

## Repository Structure

```text
.
├── airflow/                 # Airflow DAGs and orchestration assets
├── dbt/                     # dbt project, profiles, seeds, snapshots, tests
├── deploy/                  # Docker Compose files for Kafka, Spark, Airflow, and local debugging
├── scripts/                 # Helper scripts (table init, network setup, Fernet key generation)
├── src/                     # Core application code
│   ├── ingestion/           # Crawl and extraction logic
│   ├── quality_control/     # Validation schemas and error handling
│   ├── streaming/           # Kafka producer, Spark processor, monitoring
│   └── utils/               # Shared utilities such as logging
└── Makefile                 # Convenience commands for local development
```

---

## Data Flow

### 1) Ingestion
The ingestion layer crawls stock history from the source and prepares raw pandas DataFrames.  
It supports full and incremental loading modes and stores data through the pipeline for downstream processing.

### 2) Quality Control
Before the data enters the streaming layer, it is validated using Pandera schemas and custom checks.  
Invalid rows are captured, logged, and handled explicitly so the pipeline remains resilient.

### 3) Kafka Streaming
Clean records are serialized as JSON and published to Kafka topics.  
The producer is configured for safer delivery with idempotence, retries, and batching.

### 4) Spark Processing
Spark reads from Kafka, parses the payload, converts fields to proper types, and computes technical indicators.  
The processor also uses watermarking to handle late events and reduce duplicate/state issues.

### 5) Storage & Modeling
Validated and enriched data is loaded into PostgreSQL and transformed with dbt into analytical layers:
- **staging**: source normalization and incremental loading
- **intermediate**: feature engineering, gap detection, ranking, trend logic
- **marts**: final models such as stock dimensions, time dimensions, facts, and signals

---

## dbt Models

### Staging
- `stg_stock.sql`
- `stg_stock_clean.sql`

### Intermediate
- `feature_engineering.sql`
- `gap.sql`
- `rank.sql`
- `trend.sql`

### Marts
- `dim_stock.sql`
- `dim_time.sql`
- `fact_stock.sql`
- `stock_signal.sql`

---

## Data Quality Tests

dbt tests included in the project:

- `not_null_ticker.sql`
- `positive_price.sql`
- `unique_ticker_date.sql`

These tests help protect the warehouse from duplicate keys, invalid prices, and missing identifiers.

---

## Airflow

The main DAG is located at:

```text
airflow/dags/stock_data_dag.py
```

It orchestrates the main pipeline stages:

- initialize the database
- run ingestion into Kafka
- execute dbt transformation steps
- send alerts when failures occur

The DAG is designed for daily scheduling and can be extended with backfills, retries, or additional observability.

---

## Getting Started

### Prerequisites
- Docker
- Docker Compose
- Python 3.10+ (recommended)
- PostgreSQL client tools if you want to run SQL manually

### 1. Create the Docker network

```bash
bash scripts/networks.sh
```

### 2. Initialize the raw database schema

```bash
psql -h <host> -U <user> -d <db> -f scripts/create_table.sql
```

### 3. Generate an Airflow Fernet key

```bash
python scripts/generate_fernet_key.py
```

### 4. Build and start the stack

Using the Makefile:

```bash
make build
make up-infra
make up-airflow
```

Or run services individually:

```bash
docker compose -f deploy/kafka/docker-compose.kafka.yml up -d
docker compose -f deploy/spark/docker-compose.spark.yml up -d --build
docker compose -f deploy/orchestration/docker-compose.airflow.yml up -d --build
```

---

## dbt Commands

```bash
cd dbt/stock_dbt_project
dbt debug
dbt deps
dbt seed
dbt run
dbt test
```

If you are using the containerized workflow, the Makefile also provides:

```bash
make dbt-run
make dbt-test
```

---

## Docker Compose Services

### Kafka Stack
Includes:
- Kafka broker
- Python producer for debugging
- PostgreSQL warehouse
- MinIO object storage
- pgAdmin
- Kafka UI

### Spark Stack
Includes:
- Spark master
- Spark worker
- Spark streaming processor
- dbt container for transformation tasks

### Airflow Stack
Includes:
- Airflow webserver
- Airflow scheduler
- Airflow initialization container

---

## Logging

The project writes logs into dedicated folders for easier debugging:

- `logging/kafka_log/`
- `logging/spark_logs/`
- Airflow logs under `airflow/logs/`

Each service logs its own processing status, schema issues, and runtime errors.

---

## Troubleshooting

### Kafka topic not created
Check the topic creation script:

```bash
bash deploy/kafka/create-topic.sh
```

### Airflow cannot connect to Postgres
Verify:
- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`
- Airflow connection settings
- Docker network name

### dbt connection fails
Check:

```text
dbt/profiles/profiles.yml
```

and ensure the target database container is running.

### Spark dependency errors
Verify the Spark image includes the Kafka package and that the network name matches the Docker Compose configuration.

---

## Future Improvements

- Add a dashboard for pipeline health and latency
- Add alerts via Telegram or email
- Add checkpointing and replay support for streaming
- Add backfill automation
- Add more data quality checks and anomaly detection
- Add feature store or ML training outputs

---

## License

This project is released under the terms of the repository license.

---

## Author

Built as a portfolio-grade data engineering project for real-time stock market analytics.
