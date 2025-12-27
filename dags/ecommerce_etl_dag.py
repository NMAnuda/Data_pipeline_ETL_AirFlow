import sys
import os
import importlib
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'ecommerce_etl_pipeline',
    default_args=default_args,
    description='Daily E-Commerce ETL with Quality Checks',
    schedule_interval='0 2 * * *',  # Daily 2 AM
    catchup=False
)

# Helper to add path & dynamic import (no 'etl.' prefix — direct module name)
def add_etl_path_and_import(module_name):
    etl_path = '/opt/airflow/etl'
    if os.path.exists(etl_path):
        if etl_path not in sys.path:
            sys.path.insert(0, etl_path)
    else:
        local_path = os.path.join(os.path.dirname(__file__), '..', 'etl')
        if os.path.exists(local_path):
            sys.path.insert(0, local_path)
    print(f"ETL Path added: {etl_path if os.path.exists(etl_path) else local_path}")
    # Dynamic import without 'etl.' (files are in path)
    module = importlib.import_module(module_name)
    return module

# Task 1: Extract
def run_extract(**context):
    extract_module = add_etl_path_and_import('extract')
    return extract_module.extract()

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=run_extract,
    dag=dag
)

# Task 2: Transform
def run_transform(**context):
    transform_module = add_etl_path_and_import('transform')
    orders, details, targets = context['task_instance'].xcom_pull(task_ids='extract_data')
    dim_customers, dim_products, dim_date, fact_orders, fact_targets = transform_module.transform(orders, details, targets)
    return [dim_customers, dim_products, dim_date, fact_orders, fact_targets]

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=run_transform,
    dag=dag
)

# Task 3: Load
def run_load(**context):
    load_module = add_etl_path_and_import('load')
    dims = context['task_instance'].xcom_pull(task_ids='transform_data')
    load_module.load(*dims)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=run_load,
    dag=dag
)

# Task 4: Quality Checks
def run_quality(**context):
    load_module = add_etl_path_and_import('load')
    quality_module = add_etl_path_and_import('quality_checks')
    engine = load_module.get_engine()
    if not quality_module.run_quality_checks(engine):
        raise ValueError("Quality checks failed — halting DAG!")

quality_task = PythonOperator(
    task_id='quality_checks',
    python_callable=run_quality,
    dag=dag
)

# Dependencies
extract_task >> transform_task >> load_task >> quality_task