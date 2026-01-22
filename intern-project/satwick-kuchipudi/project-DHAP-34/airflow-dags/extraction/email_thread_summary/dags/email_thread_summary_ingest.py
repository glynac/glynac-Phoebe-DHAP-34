from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime
import pandas as pd
import yaml
import os
import psycopg2


BASE_PATH = "/opt/airflow/airflow-dags/extraction/email_thread_summary"
CSV_PATH = f"{BASE_PATH}/sample_data/email_thread_summary.csv"
SCHEMA_PATH = f"{BASE_PATH}/config/schema_expected.yaml"
DDL_PATH = f"{BASE_PATH}/config/create_table.sql"


def check_file():
    if not os.path.exists(CSV_PATH):
        raise AirflowFailException(f"CSV file not found at {CSV_PATH}")


def validate_schema():
    df = pd.read_csv(CSV_PATH)

    with open(SCHEMA_PATH, "r") as f:
        schema = yaml.safe_load(f)

    expected_columns = [col["name"] for col in schema["columns"]]

    if list(df.columns) != expected_columns:
        raise AirflowFailException(
            f"Schema mismatch.\nExpected: {expected_columns}\nFound: {list(df.columns)}"
        )


def transform_data():
    df = pd.read_csv(CSV_PATH)

    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].str.strip()

    df = df.replace({"": None})

    if "status" in df.columns:
        df = df[df["status"].str.lower() != "done"]

    logs_dir = os.path.join(BASE_PATH, "logs")
    os.makedirs(logs_dir, exist_ok=True)

    clean_csv_path = os.path.join(logs_dir, "clean_data.csv")
    df.to_csv(clean_csv_path, index=False)


def load_to_postgres():
    clean_csv_path = os.path.join(BASE_PATH, "logs", "clean_data.csv")

    if not os.path.exists(clean_csv_path):
        raise Exception("clean_data.csv not found. Transform step may have failed.")

    df = pd.read_csv(clean_csv_path)

    if df.empty:
        print("No rows to load. Skipping insert.")
        return

    conn = psycopg2.connect(
        host=os.getenv("LOCAL_PG_HOST"),
        port=os.getenv("LOCAL_PG_PORT"),
        dbname=os.getenv("LOCAL_PG_DB"),
        user=os.getenv("LOCAL_PG_USER"),
        password=os.getenv("LOCAL_PG_PASSWORD"),
    )

    cursor = conn.cursor()

    with open(DDL_PATH, "r") as f:
        cursor.execute(f.read())

    columns = list(df.columns)
    col_names = ",".join(columns)
    placeholders = ",".join(["%s"] * len(columns))

    insert_sql = f"""
        INSERT INTO public.email_thread_summary ({col_names})
        VALUES ({placeholders})
    """

    for _, row in df.iterrows():
        cursor.execute(insert_sql, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()


# ---------------- DAG DEFINITION ----------------

with DAG(
    dag_id="email_thread_summary_ingest",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["extraction", "csv", "postgres"],
) as dag:

    check_file_task = PythonOperator(
        task_id="check_file_exists",
        python_callable=check_file,
    )

    validate_schema_task = PythonOperator(
        task_id="validate_schema",
        python_callable=validate_schema,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    check_file_task >> validate_schema_task >> transform_task >> load_task
