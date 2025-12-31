# ETL & Analytics Pipeline

This repository contains a data pipeline project for e-commerce data. It implements incremental data ingestion, daily orchestration, data quality checks, and analytical views using a star schema.

---

## Features

1. **Incremental Ingestion**  
   Automatically loads new data from sources daily, ensuring minimal duplication and faster processing.

2. **Daily Scheduling & Orchestration**  
   Managed by Apache Airflow, the pipeline runs daily at scheduled intervals.

3. **Data Quality Checks**  
   Validation tests ensure that newly ingested and transformed data is accurate and reliable.

4. **Transformations & Star Schema**  
   ETL transforms raw data into fact and dimension tables, structured in a star schema for analytical queries.

5. **Analytical Views**  
   Materialized views and reporting tables allow quick insights on key metrics.

6. **Integration with Cloud Storage**  
   Supports S3-compatible storage (MinIO) for raw and processed data.

---

## Technologies Used

- **Python** – ETL scripts and data transformations  
- **PostgreSQL** – Data warehouse for fact and dimension tables  
- **Apache Airflow** – Workflow orchestration and scheduling  
- **dbt** – Transformation and analytics layer  
- **Docker** – Containerization of the pipeline  
- **MinIO** – S3-compatible object storage  

---

---

## Setup Instructions

### Prerequisites

- Docker & Docker Compose
- Python 3.8+
- dbt (installed in the container)


### Run Pipeline Locally

1. Start Docker services:

```bash
docker compose up -d
docker compose run --rm airflow-webserver airflow db init
docker exec -it <airflow-webserver-container> bash
cd /opt/airflow/ecommerce_dbt
dbt run --target dev

```
Notes

All fact and dimension tables are appended incrementally; previous data is preserved.

Analytical views are refreshed after every successful DAG run.

dbt models depend on fact_orders and other source tables created by the ETL scripts.

![](./image/image2.png)
![](./image/image1.png)





