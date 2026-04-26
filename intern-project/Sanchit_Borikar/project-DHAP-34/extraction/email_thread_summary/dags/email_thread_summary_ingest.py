from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import yaml
import os
from sqlalchemy import create_engine, text

# -------------------------
# Environment Variables
# -------------------------

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "airflow")
PG_USER = os.getenv("PG_USER", "airflow")
PG_PASSWORD = os.getenv("PG_PASSWORD", "airflow")

DB_URI = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# -------------------------
# File Paths (inside container)
# -------------------------

BASE_PATH = "/opt/airflow/dags"

CSV_FILE = f"{BASE_PATH}/final_dataset.csv"
SCHEMA_FILE = f"{BASE_PATH}/schema_expected.yaml"
DDL_FILE = f"{BASE_PATH}/create_table.sql"

# -------------------------
# Task 1: File Check
# -------------------------

def file_check():
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"CSV file not found at {CSV_FILE}")

# -------------------------
# Task 2: Schema Validation
# -------------------------

def validate_schema():
    df = pd.read_csv(CSV_FILE)

    with open(SCHEMA_FILE, "r") as f:
        schema = yaml.safe_load(f)

    expected_cols = [col["name"] for col in schema["columns"]]

    if list(df.columns) != expected_cols:
        raise ValueError(f"Column mismatch.\nExpected: {expected_cols}\nGot: {list(df.columns)}")

# -------------------------
# Task 3: Transform Data
# -------------------------

def transform_data():
    df = pd.read_csv(CSV_FILE)

    # Strip whitespace
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Skip already processed rows if column exists
    if "status" in df.columns:
        df = df[df["status"] != "done"]

    # Save cleaned data back
    df.to_csv(CSV_FILE, index=False)

# -------------------------
# Task 4: Load to PostgreSQL
# -------------------------

def load_to_postgres():
    df = pd.read_csv(CSV_FILE)

    engine = create_engine(DB_URI)

    # Create table using SQL file
    with open(DDL_FILE, "r") as f:
        ddl = f.read()

    with engine.connect() as conn:
        conn.execute(text(ddl))

    # Insert data
    df.to_sql("email_thread_summary", engine, if_exists="append", index=False)

# -------------------------
# DAG Definition
# -------------------------

default_args = {
    "owner": "sanchit",
    "start_date": datetime(2024, 1, 1)
}

with DAG(
    dag_id="email_thread_summary_ingest",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id="file_check",
        python_callable=file_check
    )

    t2 = PythonOperator(
        task_id="validate_schema",
        python_callable=validate_schema
    )

    t3 = PythonOperator(
        task_id="transform",
        python_callable=transform_data
    )

    t4 = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres
    )

    t1 >> t2 >> t3 >> t4
