import os
from datetime import datetime
import pandas as pd
import yaml
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DATASET_NAME = "email_thread_summaries"

# Path inside the Airflow container (you mount extraction here via docker-compose)
BASE_PATH = f"/opt/airflow/dags/extraction/{DATASET_NAME}"
CSV_PATH = f"{BASE_PATH}/sample_data/{DATASET_NAME}.csv"
SCHEMA_PATH = f"{BASE_PATH}/config/schema_expected.yaml"
DDL_PATH = f"{BASE_PATH}/config/create_table.sql"
CLEAN_PATH = f"/opt/airflow/logs/{DATASET_NAME}_cleaned.csv" # just a temp output inside repo structure

# Postgres creds (passed from docker-compose .env)
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")


def file_check():
    for p in [CSV_PATH, SCHEMA_PATH, DDL_PATH]:
        if not os.path.exists(p):
            raise FileNotFoundError(f"Missing required file: {p}")


def validate_schema():
    with open(SCHEMA_PATH, "r") as f:
        schema = yaml.safe_load(f)
    expected_cols = [c["name"] for c in schema["columns"]]

    df = pd.read_csv(CSV_PATH)

    # strict column match (names + order)
    if list(df.columns) != expected_cols:
        raise ValueError(f"Schema mismatch. Expected {expected_cols}, got {list(df.columns)}")

    # nullability + simple integer validation
    for col in schema["columns"]:
        name = col["name"]
        nullable = bool(col.get("nullable", True))
        col_type = str(col.get("type", "")).lower()

        if not nullable and df[name].isna().any():
            raise ValueError(f"Column '{name}' is NOT NULL but has nulls")

        if col_type == "integer":
            # ensure all non-null values are int-like
            non_null = df[name].dropna()
            if len(non_null) > 0:
                nums = pd.to_numeric(non_null, errors="raise")
                if (nums % 1 != 0).any():
                    raise ValueError(f"Column '{name}' has non-integer values")


def transform_data():
    df = pd.read_csv(CSV_PATH)

    # basic normalization: strip whitespace for string columns
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].astype(str).str.strip()
            df[c] = df[c].replace({"": None, "nan": None, "None": None})

    # skip rows marked "done" if status column exists
    for status_col in ["status", "dataset_status"]:
        if status_col in df.columns:
            df = df[~df[status_col].astype(str).str.lower().eq("done")]

    # write cleaned CSV (kept under extraction/<dataset>/logs/)
    os.makedirs(os.path.dirname(CLEAN_PATH), exist_ok=True)
    df.to_csv(CLEAN_PATH, index=False)


def load_to_postgres():
    # connect
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )
    cur = conn.cursor()

    # create table
    with open(DDL_PATH, "r") as ddl_file:
        cur.execute(ddl_file.read())

    # load using COPY
    with open(SCHEMA_PATH, "r") as f:
        schema = yaml.safe_load(f)

    table = schema.get("table", f"public.{DATASET_NAME}")
    cols = [c["name"] for c in schema["columns"]]

    copy_sql = f"COPY {table} ({', '.join(cols)}) FROM STDIN WITH CSV HEADER"

    with open(CLEAN_PATH, "r") as f:
        cur.copy_expert(copy_sql, f)

    conn.commit()
    cur.close()
    conn.close()


def verify_load():
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )
    cur = conn.cursor()

    with open(SCHEMA_PATH, "r") as f:
        schema = yaml.safe_load(f)

    table = schema.get("table", f"public.{DATASET_NAME}")

    cur.execute(f"SELECT COUNT(*) FROM {table};")
    count = cur.fetchone()[0]

    cur.close()
    conn.close()

    print(f"Loaded row count in {table}: {count}")
    return int(count)


with DAG(
    dag_id=f"{DATASET_NAME}_ingest",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["DHAP-34", "story3"],
) as dag:

    t1 = PythonOperator(task_id="file_check", python_callable=file_check)
    t2 = PythonOperator(task_id="validate_schema", python_callable=validate_schema)
    t3 = PythonOperator(task_id="transform", python_callable=transform_data)
    t4 = PythonOperator(task_id="load", python_callable=load_to_postgres)
    t5 = PythonOperator(task_id="verify", python_callable=verify_load)

    t1 >> t2 >> t3 >> t4 >> t5