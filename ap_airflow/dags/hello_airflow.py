from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

def greet():
    logging.getLogger("airflow.task").info("Hello, Airflow! %s", datetime.now())
    logging.getLogger("airflow.task").info("This is from greet function inside task of a dag %s", datetime.now())

def bye():
    logging.getLogger("airflow.task").info("Goodbye, Airflow! %s", datetime.now())
    logging.getLogger("airflow.task").info("example of proper DAG with multiple task %s", datetime.now())
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=20),
}

with DAG(
    dag_id="hello_airflow",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule="@daily",           # Airflow 3 uses `schedule`
    catchup=False,
    tags=["demo"],
) as dag:
    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=greet,
    )
    
    bye_task = PythonOperator(
        task_id="say_bye",
        python_callable=bye,
    )
    
    hello_task >> bye_task
