import os
import ast
import yaml
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_PATH, "customer_care_emails_sample.csv")
SCHEMA_PATH = os.path.join(BASE_PATH, "..", "config", "schema.yaml")
DDL_PATH = os.path.join(BASE_PATH, "create_table.sql")
LOG_DIR = os.path.join(BASE_PATH, "logs")

def get_pg_conn():
    return psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="airflow",
        user="airflow",
        password="airflow"
    )

def check_file_exists():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV file not found at {CSV_PATH}")

def validate_schema():
    if not os.path.exists(SCHEMA_PATH):
        raise FileNotFoundError(f"Schema file not found at {SCHEMA_PATH}")
        
    df = pd.read_csv(CSV_PATH)
    with open(SCHEMA_PATH, "r") as f:
        schema = yaml.safe_load(f)

    expected_columns = schema["columns"]
    df_columns = [c.strip().lower() for c in df.columns]

    for col in expected_columns:
        name = col["name"].strip().lower()
        nullable = col.get("nullable", True) 

        if name not in df_columns:
            raise ValueError(f"Missing column: {name}")

        original_name = next(c for c in df.columns if c.strip().lower() == name)
        if not nullable and df[original_name].isnull().any():
            raise ValueError(f"Nulls found in column: {original_name}")

    print("Schema validation passed!")

def transform_data(**context):
    df = pd.read_csv(CSV_PATH)
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()

    df = df[df["email_status"] != "completed"]

    array_cols = ["email_types", "product_types"]
    def to_list(val):
        if pd.isna(val): return []
        s = str(val).strip()
        try:
            parsed = ast.literal_eval(s)
            return parsed if isinstance(parsed, list) else [s]
        except:
            return [x.strip() for x in s.split(",")] if s else []

    for col in array_cols:
        if col in df.columns:
            df[col] = df[col].apply(to_list)

    os.makedirs(LOG_DIR, exist_ok=True)
    transformed_path = os.path.join(LOG_DIR, "cleaned.csv")
    df.to_csv(transformed_path, index=False)
    context["ti"].xcom_push(key="transformed_path", value=transformed_path)

def load_to_postgres(**context):
    transformed_path = context["ti"].xcom_pull(key="transformed_path")
    df = pd.read_csv(transformed_path)

    array_cols = ["email_types", "product_types"]
    def to_list(val):
        if pd.isna(val): return []
        s = str(val).strip()
        try:
            parsed = ast.literal_eval(s)
            return parsed if isinstance(parsed, list) else [s]
        except:
            return [x.strip() for x in s.split(",")] if s else []

    for col in array_cols:
        if col in df.columns:
            df[col] = df[col].apply(to_list)

    conn = get_pg_conn()
    cur = conn.cursor()
    with open(DDL_PATH, "r") as f:
        cur.execute(f.read())
    conn.commit()

    columns = list(df.columns)
    placeholders = ",".join(["%s"] * len(columns))
    insert_sql = f"INSERT INTO public.customer_care_emails ({','.join(columns)}) VALUES ({placeholders}) ON CONFLICT (thread_id) DO NOTHING"

    for _, row in df.iterrows():
        cur.execute(insert_sql, tuple(row))
    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id="customer_care_emails_ingest",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["customer_care", "csv", "postgres"],
) as dag:
    t1 = PythonOperator(task_id="check_csv_exists", python_callable=check_file_exists)
    t2 = PythonOperator(task_id="validate_schema", python_callable=validate_schema)
    t3 = PythonOperator(task_id="transform_data", python_callable=transform_data)
    t4 = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)
    t1 >> t2 >> t3 >> t4