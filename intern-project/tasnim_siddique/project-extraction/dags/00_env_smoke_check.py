from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="env_smoke_check",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["smoke", "env"],
) as dag:
    EmptyOperator(task_id="it_runs")
