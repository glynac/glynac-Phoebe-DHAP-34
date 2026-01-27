import os
import yaml
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# --------------------
# Paths
# --------------------

DATASET_NAME = "education_dialogue_dataset"
BASE_PATH = f"/opt/airflow/extraction/{DATASET_NAME}"

CSV_PATH = f"{BASE_PATH}/sample_data/education-dialogue-dataset.csv"
SCHEMA_PATH = f"{BASE_PATH}/config/schema_expected.yaml"
DDL_PATH = f"{BASE_PATH}/config/create_table.sql"
LOG_DIR = f"{BASE_PATH}/logs"


# --------------------
# Helpers
# --------------------

def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("EXT_PG_HOST"),
        port=os.getenv("EXT_PG_PORT", 5432),
        dbname=os.getenv("EXT_PG_DB"),
        user=os.getenv("EXT_PG_USER"),
        password=os.getenv("EXT_PG_PASSWORD"),
        sslmode=os.getenv("EXT_PG_SSLMODE", "prefer"),
    )


# --------------------
# Tasks
# --------------------

def check_file_exists():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV file not found at {CSV_PATH}")


def validate_schema():
    df = pd.read_csv(CSV_PATH)

    with open(SCHEMA_PATH, "r") as f:
        schema = yaml.safe_load(f)

    expected_columns = schema["columns"]
    df_columns = set(df.columns)

    for col in expected_columns:
        name = col["name"]
        nullable = col["nullable"]

        if name not in df_columns:
            raise ValueError(f"Missing column in CSV: {name}")

        if not nullable and df[name].isnull().any():
            raise ValueError(f"Null values found in non-nullable column: {name}")


def transform_data(**context):
    df = pd.read_csv(CSV_PATH)

    # Normalize string columns
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()

    os.makedirs(LOG_DIR, exist_ok=True)
    transformed_path = f"{LOG_DIR}/transformed.csv"
    df.to_csv(transformed_path, index=False)

    context["ti"].xcom_push(key="transformed_path", value=transformed_path)


def load_to_postgres(**context):
    transformed_path = context["ti"].xcom_pull(key="transformed_path")
    df = pd.read_csv(transformed_path)

    conn = get_pg_conn()
    cur = conn.cursor()

    # Create table
    with open(DDL_PATH, "r") as f:
        cur.execute(f.read())

    conn.commit()

    columns = list(df.columns)
    placeholders = ",".join(["%s"] * len(columns))

    insert_sql = f"""
        INSERT INTO public.education_dialogue ({','.join(columns)})
        VALUES ({placeholders})
    """

    for _, row in df.iterrows():
        cur.execute(insert_sql, tuple(row))

    conn.commit()
    cur.close()
    conn.close()


# --------------------
# DAG
# --------------------

with DAG(
    dag_id="education_dialogue_ingest",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["education", "dialogue", "csv", "postgres"],
) as dag:

    t1_check_file = PythonOperator(
        task_id="check_csv_exists",
        python_callable=check_file_exists,
    )

    t2_validate = PythonOperator(
        task_id="validate_schema",
        python_callable=validate_schema,
    )

    t3_transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True,
    )

    t4_load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
        provide_context=True,
    )

    t1_check_file >> t2_validate >> t3_transform >> t4_load
