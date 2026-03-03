# NexFlow — Supply Chain Data Engineering Pipeline

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.4-black)
![Snowflake](https://img.shields.io/badge/Snowflake-Cloud%20DW-lightblue)
![dbt](https://img.shields.io/badge/dbt-1.11-orange)
![Airflow](https://img.shields.io/badge/Airflow-2.8-green)
![AWS](https://img.shields.io/badge/AWS-S3-yellow)

## Overview

NexFlow is an end-to-end supply chain data engineering pipeline that streams 50,000+ orders in real time, processes them through a multi-layer ETL pipeline, stores data in a cloud data lake and warehouse, runs 3 ML models, and visualizes insights in Tableau.

---

## Architecture
```
Orders → Kafka → ETL → SQL Server
                   ↓
             AWS S3 (Medallion)
             ├── Bronze (raw)
             ├── Silver (clean)
             └── Gold (business ready)
                   ↓
              Snowflake (DW)
                   ↓
                dbt models
                   ↓
              ML Models
                   ↓
              Tableau
                   ↓
              Airflow (orchestration)
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Streaming | Apache Kafka |
| ETL | Python, pandas |
| Data Lake | AWS S3 (Medallion Architecture) |
| Data Warehouse | Snowflake + SQL Server |
| Transformation | dbt (8 automated tests) |
| Orchestration | Apache Airflow |
| ML Models | XGBoost, Random Forest, Isolation Forest |
| Visualization | Tableau |
| Containerization | Docker |
| Distributed Computing | Databricks + PySpark (In Progress) |

---

## Key Results

- **50,000+** orders streamed through Kafka in real time
- **3,643** bad rows caught and removed by ETL pipeline
- **$150,000+** in inventory savings identified
- **XGBoost** demand forecasting — MAE: 15.7 units (MAPE: 8.4%)
- **Random Forest** churn prediction — 70.3% accuracy
- **Isolation Forest** anomaly detection — 2,593 suspicious orders flagged
- **8** automated dbt data quality tests — all passing
- **Medallion Architecture** — Bronze/Silver/Gold layers in S3

---

## Project Structure
```
NexFlow/
├── config/                 # Database + AWS credentials
├── streaming/              # Kafka producer + consumer
│   ├── producer.py
│   └── consumer.py
├── etl/                    # ETL pipeline
│   ├── extract.py
│   ├── transform.py
│   └── load.py
├── ml/                     # ML models
│   ├── demand_forecast.py  # XGBoost
│   ├── churn_prediction.py # Random Forest
│   └── anomaly_detection.py# Isolation Forest
├── nexflow_dbt/            # dbt transformation layer
│   └── models/
│       ├── staging/        # stg_orders, stg_customers
│       └── marts/          # customer_summary
├── sql/                    # SQL Server stored procedures
│   └── stored_procedures.sql
├── docker/                 # Docker compose for Kafka
├── generate_data.py        # Synthetic data generation
├── run_pipeline.py         # ETL orchestration
├── s3_upload.py            # AWS S3 upload
├── snowflake_load.py       # Snowflake loader
├── medallion_architecture.py # Bronze/Silver/Gold layers
└── export_for_tableau.py   # Tableau data export
```

---

## Pipeline Stages

### Stage 1: Data Generation
- Generated 5,000 customers, 500 products, 10 warehouses
- 55,500 orders with intentional data quality issues
- Tools: Python, Faker

### Stage 2: Real-Time Streaming
- Kafka producer streams orders to `orders` topic
- Kafka consumer reads and processes messages
- Docker runs Kafka + Zookeeper containers

### Stage 3: ETL Pipeline
- **Extract:** Read raw CSVs
- **Transform:** Remove duplicates, fix nulls, reject negatives, quarantine orphans
- **Load:** Clean data → SQL Server star schema
- Result: 3,643 bad rows removed, 334 issues fixed

### Stage 4: Cloud Data Lake (AWS S3)
- **Bronze:** Raw data exactly as generated
- **Silver:** Cleaned and validated data
- **Gold:** Aggregated business metrics

### Stage 5: Cloud Data Warehouse (Snowflake)
- Star schema replicated in Snowflake
- dbt models for transformation
- 8 automated data quality tests

### Stage 6: ML Models
| Model | Algorithm | Result |
|-------|-----------|--------|
| Demand Forecasting | XGBoost | MAE: 15.7 units |
| Churn Prediction | Random Forest | 70.3% accuracy |
| Anomaly Detection | Isolation Forest | 2,593 flagged |

### Stage 7: Orchestration (Airflow)
- DAG runs every day at 6 AM
- 7 tasks with dependency management
- Auto-retry on failure

---

## Setup Instructions

### Prerequisites
```bash
Python 3.11+
Docker Desktop
AWS Account
Snowflake Account
```

### Installation
```bash
git clone https://github.com/ZbrAbd/NexFlow.git
cd NexFlow
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### Environment Variables
Create `.env` file:
```
AWS_ACCESS_KEY=your_key
AWS_SECRET_KEY=your_secret
AWS_REGION=us-east-2
S3_BUCKET=your_bucket
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=NEXFLOW
SNOWFLAKE_SCHEMA=WAREHOUSE
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
DB_SERVER=your_server
DB_NAME=NexFlow
```

### Run Pipeline
```bash
# Start Kafka
docker-compose -f docker/docker-compose.yml up -d

# Generate data
python generate_data.py

# Run ETL
python run_pipeline.py

# Upload to S3
python medallion_architecture.py

# Load Snowflake
python snowflake_load.py

# Run dbt
cd nexflow_dbt
dbt run
dbt test

# Run ML models
python ml/demand_forecast.py
python ml/churn_prediction.py
python ml/anomaly_detection.py
```

---

## Roadmap

- [x] Phase 1: Kafka + ETL + SQL Server + ML + Tableau
- [x] Phase 2: AWS S3 + Snowflake + dbt + Airflow + Medallion
- [ ] Phase 3: Databricks + PySpark (In Progress)
- [ ] Phase 4: Great Expectations data quality framework

---

## Author

**Zubair** 
- GitHub: [@ZbrAbd](https://github.com/ZbrAbd)