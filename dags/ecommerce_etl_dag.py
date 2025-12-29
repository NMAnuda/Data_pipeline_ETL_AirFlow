import sys
import os
import importlib
import subprocess
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import text

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
    description='Daily Incremental E-Commerce ETL with Quality & Analytics',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False
)

# Helper: Add ETL path and import module dynamically
def add_etl_path_and_import(module_name):
    etl_path = '/opt/airflow/etl'
    if os.path.exists(etl_path) and etl_path not in sys.path:
        sys.path.insert(0, etl_path)
    module = importlib.import_module(module_name)
    return module

# Task 1: Check for new data
def check_new_data(**context):
    load_module = add_etl_path_and_import('load')
    engine = load_module.get_engine()
    incremental_module = add_etl_path_and_import('incremental')
    last_date = incremental_module.get_last_load_date(engine)
    extract_module = add_etl_path_and_import('extract')
    orders, _, _ = extract_module.extract()
    new_data_count = len(orders)
    if new_data_count == 0:
        print("No new data — skipping ETL.")
        return False
    print(f"New data: {new_data_count} records since {last_date}.")
    return True

check_task = PythonOperator(
    task_id='check_new_data',
    python_callable=check_new_data,
    dag=dag
)

# Task 2: Extract
def run_extract(**context):
    extract_module = add_etl_path_and_import('extract')
    return extract_module.extract()

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=run_extract,
    dag=dag
)

# Task 3: Transform
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

# Task 4: Load
def run_load(**context):
    load_module = add_etl_path_and_import('load')
    dims = context['task_instance'].xcom_pull(task_ids='transform_data')
    load_module.load(*dims)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=run_load,
    dag=dag
)

# Task 5: Quality Checks
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

# Task 6: Run dbt
def run_dbt():
    import subprocess
    import os

    dbt_dir = "/opt/airflow/ecommerce_dbt"
    os.environ["DBT_PROFILES_DIR"] = dbt_dir

    result = subprocess.run(
        ["dbt", "run", "--project-dir", dbt_dir, "--profiles-dir", dbt_dir],
        capture_output=True,
        text=True
    )

    print(result.stdout)

    if result.returncode != 0:
        raise RuntimeError(result.stderr)


dbt_task = PythonOperator(
    task_id='run_dbt',
    python_callable=run_dbt,
    dag=dag
)

# Task 7: Refresh Analytical Views
def refresh_views(**context):
    load_module = add_etl_path_and_import('load')
    engine = load_module.get_engine()
    with engine.begin() as conn:
        conn.execute(text("REFRESH MATERIALIZED VIEW monthly_revenue_view;"))
        conn.execute(text("REFRESH MATERIALIZED VIEW top_customers_view;"))
    print("Analytical views refreshed! ✅")

views_task = PythonOperator(
    task_id='refresh_analytical_views',
    python_callable=refresh_views,
    dag=dag
)

# Set dependencies
check_task >> extract_task >> transform_task >> load_task >> quality_task >> dbt_task >> views_task
