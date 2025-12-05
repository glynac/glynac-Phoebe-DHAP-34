import os
import pandas as pd
import yaml
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Paths relative to extraction/customer_care_emails/
DATASET_NAME = "customer_care_emails"
BASE_PATH = "/opt/airflow/extraction/customer_care_emails"
CSV_PATH = f"{BASE_PATH}/sample_data/customer_care_emails_sample.csv"
SCHEMA_PATH = f"{BASE_PATH}/config/schema_expected.yaml"
DDL_PATH = f"{BASE_PATH}/config/create_table.sql"

# Load env vars (injected via Docker Compose)
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

def file_check():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV file not found at {CSV_PATH}")

def validate_schema():
    # Load schema contract
    with open(SCHEMA_PATH, "r") as f:
        schema = yaml.safe_load(f)

    expected_cols = [col["name"] for col in schema["columns"]]
    df = pd.read_csv(CSV_PATH)

    # Check columns
    if list(df.columns) != expected_cols:
        raise ValueError(f"Schema mismatch! Expected {expected_cols}, got {list(df.columns)}")

def transform_data():
    df = pd.read_csv(CSV_PATH)

    # Strip whitespace and fill NaN
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df = df.fillna("")

    output_path = f"{BASE_PATH}/logs/cleaned.csv"
    df.to_csv(output_path, index=False)
    print(f"Cleaned data written to: {output_path}")

def load_to_postgres():
    print(f"Connecting to Postgres at {PG_HOST}:{PG_PORT}, db={PG_DB}, user={PG_USER}")
    df = pd.read_csv(f"{BASE_PATH}/logs/cleaned.csv")
    print(f"Loaded {len(df)} rows from cleaned.csv")

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD
    )
    cur = conn.cursor()

    # Ensure table exists
    with open(DDL_PATH, "r") as ddl_file:
        cur.execute(ddl_file.read())

    # Insert rows with explicit mapping
    for _, row in df.iterrows():
      try:
        cur.execute("""
            INSERT INTO public.customer_care_emails
            (subject, sender, receiver, timestamp, message_body, thread_id,
             email_types, email_status, email_criticality, product_types,
             agent_effectivity, agent_efficiency, customer_satisfaction)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            row["subject"], row["sender"], row["receiver"], row["timestamp"],
            row["message_body"], row["thread_id"], row["email_types"],
            row["email_status"], row["email_criticality"], row["product_types"],
            row["agent_effectivity"], row["agent_efficiency"], row["customer_satisfaction"]
        ))
      except Exception as e:
        print(f"Failed to insert row {row.to_dict()}: {e}")

    conn.commit()
    cur.close()
    conn.close()

# Define DAG
with DAG(
    dag_id="customer_care_emails_ingest",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id="file_check", python_callable=file_check)
    t2 = PythonOperator(task_id="validate_schema", python_callable=validate_schema)
    t3 = PythonOperator(task_id="transform", python_callable=transform_data)
    t4 = PythonOperator(task_id="load", python_callable=load_to_postgres)

    t1 >> t2 >> t3 >> t4
