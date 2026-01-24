default_args = {
    "owner": "airflow"
}

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

DATA_PATH = "/opt/airflow/data/"

PG_CONN = "postgresql://airflow:airflow@postgres:5432/airflow"

def load_csv_to_postgres():

    engine = create_engine(PG_CONN)

    details = pd.read_csv(f"{DATA_PATH}email_thread_details.csv")
    summaries = pd.read_csv(f"{DATA_PATH}email_thread_summaries.csv")

    details.to_sql("email_thread_details", engine, if_exists='append', index=False)
    summaries.to_sql("email_thread_summaries", engine, if_exists='append', index=False)

with DAG(
    dag_id="load_email_thread_data",
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False
):
    load_task = PythonOperator(
        task_id="load_email_data",
        python_callable=load_csv_to_postgres
    )
