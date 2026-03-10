
# Spark Technical Test – Complete Setup, Batch ETL & Streaming Guide

This README provides end‑to‑end instructions for setting up the environment, running the batch ETL job, and executing the Spark Streaming job using Kafka/Redpanda. It consolidates all provided files.

## 0) Local Setup on macOS (Apple Silicon or Intel)

### 0.1 Install prerequisites
```bash
# Homebrew (if not installed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Java & Python tooling
brew install openjdk@17 python@3.11

# Docker Desktop (or Colima)
# https://www.docker.com/products/docker-desktop/
```
Spark 3.5.x runs fine on Java 17.

### 0.2 Create the project skeleton
```bash
mkdir spark-tech-test && cd spark-tech-test
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install "pyspark==3.5.1" "pyyaml==6.0.2"
```
Kafka & JDBC dependencies are pulled automatically.

---

## 1) Infra with Docker (MySQL + Redpanda)
Create `docker-compose.yml` and start services:
```bash
docker compose up -d
```
Containers started:
- MySQL (port 3306)
- Redpanda broker (Kafka‑compatible, port 19092)
- Redpanda Console (port 8080)

---

## 2) Seed the MySQL Database
Your `init.sql` file:
```sql
CREATE TABLE IF NOT EXISTS transactions (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  user_id BIGINT NOT NULL,
  amount DECIMAL(12,2) NOT NULL,
  currency VARCHAR(3) NOT NULL DEFAULT 'AED',
  channel VARCHAR(20) NOT NULL,
  status VARCHAR(20) NOT NULL DEFAULT 'SUCCESS',
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
INSERT INTO transactions ...;
```
Load data:
```bash
docker cp sql/init.sql mysql:/init.sql
docker exec -it mysql bash -lc 'mysql -uroot -proot salesdb < /init.sql'
```

---

## 3) Exercise 1 — Batch ETL (Config‑Driven & Incremental)

### 3.1 `configs/batch_config.yml`
Contains JDBC config, transform logic (rename, formulas, static columns, drop columns), select list, and checkpoint file.

### 3.2 Run batch ETL
```bash
python apps/etl_batch.py --config configs/batch_config.yml
```
Results written to:
```
data/out/batch/transactions
```
Checkpoint stored at:
```
.checkpoints/batch_etl.json
```
Re-run the job after inserting new rows — only incremental rows are processed.

---

## 4) Exercise 2 — Spark Streaming from Kafka

### 4.1 Create topics
```bash
docker exec -it redpanda rpk topic create transactions_in
docker exec -it redpanda rpk topic create batch_metrics
```

### 4.2 `configs/stream_config.yml`
Defines Kafka input, metrics topic, schema, transform rules, and sink.

### 4.3 Run streaming job
```bash
python apps/streaming_kafka.py --config configs/stream_config.yml
```
Writes data to parquet & publishes batch metrics to Kafka.

### 4.4 Produce test messages
```bash
docker exec -it redpanda rpk topic produce transactions_in
```

---

## 5) Kafka Connector JAR Setup (Optional)
Use provided `setup_kafka_connectors.sh` to auto-download Kafka packages:
```bash
bash scripts/setup_kafka_connectors.sh
source .env.pyspark
```

---

## Directory Structure
```
spark-tech-test/
  apps/
    etl_batch.py
    streaming_kafka.py
  configs/
    batch_config.yml
    stream_config.yml
  scripts/
    setup_kafka_connectors.sh
  sql/
    init.sql
  data/
    out/
```

---

## Completed Capabilities
- Incremental JDBC ingestion
- Config-driven transforms
- Static & formula-derived columns
- Selective output projection
- Parquet/CSV sinks
- Spark Streaming with Kafka
- DLQ handling and per‑batch metrics

---

## How to Run Everything
1. Start Docker: `docker compose up -d`
2. Seed DB: run MySQL load command
3. Run batch ETL
4. Produce Kafka messages & run streaming consumer

---

