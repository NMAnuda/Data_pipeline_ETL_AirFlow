import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append('/opt/airflow/etl')  # Fix for Docker path

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from etl.extract import extract
from etl.transform import transform
from etl.load import load

# Default args for retries & owner
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'ecommerce_etl_pipeline',
    default_args=default_args,
    description='Daily E-Commerce ETL Pipeline',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False
)

# Task 1: Extract
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract,  # Fixed: No lambda needed
    dag=dag
)

# Task 2: Transform
def run_transform(**context):
    orders, details, targets = context['task_instance'].xcom_pull(task_ids='extract_data')
    dim_customers, dim_products, dim_date, fact_orders, fact_targets = transform(orders, details, targets)
    return [dim_customers, dim_products, dim_date, fact_orders, fact_targets]

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=run_transform,
    dag=dag
)

# Task 3: Load
def run_load(**context):
    dims = context['task_instance'].xcom_pull(task_ids='transform_data')
    load(*dims)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=run_load,
    dag=dag
)

# Set task dependencies
extract_task >> transform_task >> load_task