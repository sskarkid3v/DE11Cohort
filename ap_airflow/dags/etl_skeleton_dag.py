"""
Basic ETL DAG skeleton for Airflow .

Goal:
- Show common structure for Extract → Transform → Load pipelines
- Demonstrate clear task boundaries and dependencies
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# --------------------------------------------------------------------
# 1️⃣  Placeholder functions for each ETL step
# --------------------------------------------------------------------

def extract(**context):
    """
    Simulate pulling data from a source.
    Replace this with API calls, database queries, etc.
    """
    print("Extracting data...")
    # fake data id to pass along
    return "batch_2025_09_22"


def transform(ti):
    """
    Simulate data cleaning or transformation.
    Pulls the batch_id from XCom.
    """
    batch_id = ti.xcom_pull(task_ids="extract_task")
    print(f"Transforming data for {batch_id}")
    # return a fake row count
    return 42


def load(ti):
    """
    Simulate loading processed data to a warehouse or lake.
    Receives row count from transform step.
    """
    row_count = ti.xcom_pull(task_ids="transform_task")
    print(f"Loading {row_count} rows into destination")


# --------------------------------------------------------------------
# 2️⃣  DAG definition
# --------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="etl_skeleton",
    description="Skeleton DAG showing typical ETL shape",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",   # run once per day
    catchup=False,
    tags=["training", "etl"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load,
    )

    # ----------------------------------------------------------------
    # 3️⃣  Set dependencies: Extract → Transform → Load
    # ----------------------------------------------------------------
    extract_task >> transform_task >> load_task
    
    
