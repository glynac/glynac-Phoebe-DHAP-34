from datetime import datetime
import os
import pandas as pd
import yaml
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
SAMPLE_DATA = os.path.join(BASE_DIR, "sample_data", "customer_care_emails.csv")
SCHEMA_FILE = os.path.join(BASE_DIR, "config", "schema_expected.yaml")
DDL_FILE = os.path.join(BASE_DIR, "config", "create_table.sql")
LOG_DIR = os.path.join(BASE_DIR, "logs")

def file_check():
    if not os.path.exists(SAMPLE_DATA):
        raise FileNotFoundError(f"CSV file not found: {SAMPLE_DATA}")

def validate_and_transform():
    with open(SCHEMA_FILE, "r") as f:
        schema = yaml.safe_load(f)

    df = pd.read_csv(SAMPLE_DATA)

    # --- Strip whitespace (no applymap) ---
    str_cols = df.select_dtypes(include=["object", "string"]).columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())

    # Null handling
    df = df.replace({"": pd.NA, "null": pd.NA, "NULL": pd.NA})

    # Status skip (drop rows where any *status* col == "done")
    status_cols = [c for c in df.columns if "status" in c.lower()]
    for sc in status_cols:
        df = df[df[sc].astype("string").str.lower() != "done"]

    # Type coercion based on schema
    for col in schema["columns"]:
        colname = col["name"]
        if colname not in df.columns:
            raise ValueError(f"Missing column {colname}")

        typ = col["type"].lower()
        if typ == "integer":
            df[colname] = pd.to_numeric(df[colname], errors="coerce").astype("Int64")
        elif typ == "float":
            df[colname] = pd.to_numeric(df[colname], errors="coerce")
        elif typ == "boolean":
            # robust boolean parsing
            true_vals = {"true","t","1","yes","y"}
            false_vals = {"false","f","0","no","n"}
            df[colname] = (
                df[colname]
                .astype("string")
                .str.lower()
                .map(lambda v: True if v in true_vals else False if v in false_vals else pd.NA)
                .astype("boolean")
            )
        elif typ == "timestamp":
            df[colname] = pd.to_datetime(df[colname], errors="coerce")

        if not col.get("nullable", True) and df[colname].isna().any():
            raise ValueError(f"Column {colname} contains NULLs but is not nullable.")

    # Save cleaned file
    os.makedirs(LOG_DIR, exist_ok=True)
    cleaned_path = os.path.join(
        LOG_DIR,
        f"cleaned_customer_care_emails_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv",
    )
    df.to_csv(cleaned_path, index=False)
    return cleaned_path


def create_table_if_not_exists():
    hook = PostgresHook(postgres_conn_id="target_postgres")
    conn = hook.get_conn()
    cursor = conn.cursor()
    with open(DDL_FILE, "r") as f:
        ddl = f.read()
    cursor.execute(ddl)
    conn.commit()
    cursor.close()
    conn.close()

def load_to_postgres(cleaned_path, **context):
    hook = PostgresHook(postgres_conn_id="target_postgres")
    conn = hook.get_conn()
    cursor = conn.cursor()
    with open(cleaned_path, "r") as f:
        cursor.copy_expert("COPY public.customer_care_emails FROM STDIN WITH CSV HEADER", f)
    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id="customer_care_emails_ingest",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["customer_care_emails", "ingest"],
) as dag:

    t1 = PythonOperator(task_id="file_check", python_callable=file_check)
    t2 = PythonOperator(task_id="validate_and_transform", python_callable=validate_and_transform)
    t3 = PythonOperator(task_id="create_table_if_not_exists", python_callable=create_table_if_not_exists)
    t4 = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres, op_kwargs={"cleaned_path": "{{ ti.xcom_pull(task_ids='validate_and_transform') }}"})

    t1 >> t2 >> t3 >> t4
