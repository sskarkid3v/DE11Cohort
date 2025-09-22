from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import random

def start():
    print("Pipeline starting…")
    return "kickoff complete"

def choose_path(**context):
    path = random.choice(["error_task", "task_b"])
    print(f"Branching to: {path}")
    return path

def error_task():
    if random.random() < 0.5:
        raise ValueError("Random failure - please retry")
    print("Task succeeded this time")

#def path_a():
#    print("Executing Task A logic")

def path_b():
    print("Executing Task B logic")

def wrap_up(ti):
    message = ti.xcom_pull(task_ids="start")
    print(f"Finishing pipeline – value from start(): {message}")

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="advanced_demo",
    default_args=default_args,
    description="Demo of branching, retries, and XCom",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["training"],
) as dag:
    kick_off = PythonOperator(task_id="start", python_callable=start)
    branch = BranchPythonOperator(task_id="branch", python_callable=choose_path)
    #t_a = PythonOperator(task_id="task_a", python_callable=path_a)
    t_b = PythonOperator(task_id="task_b", python_callable=path_b)
    unstable = PythonOperator(task_id="error_task", python_callable=error_task)
    join = EmptyOperator(task_id="join", trigger_rule="none_failed_min_one_success")
    end = PythonOperator(task_id="finish", python_callable=wrap_up)

    kick_off >> branch
    branch >> [unstable, t_b] >> join >> end
