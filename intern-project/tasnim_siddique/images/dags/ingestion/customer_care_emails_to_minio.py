# dags/ingestion/customer_care_emails_to_minio.py
from __future__ import annotations

import os
import shutil
import datetime as dt
from typing import Dict, Any

import pandas as pd
import yaml
import boto3
from botocore.client import Config
import pyarrow as pa
import pyarrow.parquet as pq

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator


CSV_PATH = "/opt/airflow/dags/extraction/customer_care_emails.csv"  
SCHEMA_PATH = "/opt/airflow/ingestion/customer_care_emails/config/schema_expected.yaml"
LOCAL_OUT_BASE = "/opt/airflow/ingestion/customer_care_emails/_parquet_out"

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bronze")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
TARGET_PREFIX = os.getenv("MINIO_PREFIX", "customer_care_emails")


def _load_schema(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise AirflowFailException(f"Schema file not found at {path}")
    import yaml
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


_PANDAS_COERCE = {
    "integer": "Int64",
    "bigint": "Int64",
    "numeric": "float64",
    "double": "float64",
    "float": "float64",
    "boolean": "boolean",
    "string": "string",
    "text": "string",
    "timestamp": "datetime64[ns]",
    "date": "datetime64[ns]",
}

_PANDAS_KIND_FAMILY = {
    "integer": {"i", "u"},
    "bigint": {"i", "u"},
    "numeric": {"i", "u", "f"},
    "double": {"f"},
    "float": {"f"},
    "boolean": {"b"},
    "string": {"O", "S", "U"},
    "text": {"O", "S", "U"},
    "timestamp": {"M"},
    "date": {"M"},
}


def _assert_file_exists():
    if not os.path.exists(CSV_PATH):
        raise AirflowFailException(f"Input CSV not found at {CSV_PATH}")


def _validate_schema(**context):
    schema = _load_schema(SCHEMA_PATH)
    df = pd.read_csv(CSV_PATH)

    cols = schema.get("columns", [])
    expected = [c["name"] for c in cols]
    enforce_order = bool(schema.get("enforce_column_order", True))
    allow_unknown = bool(schema.get("allow_unknown_columns", False))

    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise AirflowFailException(f"Missing columns: {missing}")

    if not allow_unknown:
        unknown = [c for c in df.columns if c not in expected]
        if unknown:
            raise AirflowFailException(f"Unknown columns present: {unknown}")

    if enforce_order and list(df.columns) != expected:
        raise AirflowFailException(
            f"Column order mismatch. Expected: {expected}  Actual: {list(df.columns)}"
        )

    for c in cols:
        name = c["name"]
        exp_type = str(c.get("type", "string")).lower()
        nullable = bool(c.get("nullable", True))

        if not nullable and df[name].isna().any():
            raise AirflowFailException(f"Non-nullable column `{name}` contains nulls.")

        kind = df[name].dtype.kind
        acceptable = _PANDAS_KIND_FAMILY.get(exp_type, {"O"})
        if kind not in acceptable:
            if exp_type in {"timestamp", "date"}:
                try:
                    pd.to_datetime(df[name], errors="raise")
                    continue
                except Exception:
                    pass
            raise AirflowFailException(
                f"Type mismatch for `{name}`: got pandas kind `{kind}`, expected family {acceptable}."
            )

    pk = schema.get("primary_key", [])

    if pk:
     cols = pk if isinstance(pk, list) else [pk]

    # any null in any PK column?
    if df[cols].isna().any().any():
        raise AirflowFailException(f"Primary key {cols} contains nulls.")

    # any duplicate across the PK column set?
    if df.duplicated(subset=cols).any():
        raise AirflowFailException(f"Primary key {cols} has duplicates.")


    context['ti'].xcom_push(key='row_count', value=len(df))


def _transform_and_write_parquet(**context):
    schema = _load_schema(SCHEMA_PATH)
    df = pd.read_csv(CSV_PATH)

    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype('string').str.strip()

    for col in schema.get('columns', []):
        name = col['name']
        exp_type = str(col.get('type', 'string')).lower()
        if name not in df.columns:
            continue
        target = _PANDAS_COERCE.get(exp_type)
        if exp_type in {'timestamp', 'date'}:
            df[name] = pd.to_datetime(df[name], errors='coerce')
        elif target:
            if target == 'boolean':
                df[name] = (
                    df[name].astype('string').str.lower()
                    .map({'true': True, 'false': False, '1': True, '0': False})
                ).astype('boolean')
            else:
                df[name] = df[name].astype(target)

        if not bool(col.get('nullable', True)) and df[name].isna().any():
            raise AirflowFailException(f"Non-nullable column `{name}` has nulls after casting.")

    ingest_date = dt.date.today().isoformat()
    df['ingest_date'] = ingest_date

    if os.path.exists(LOCAL_OUT_BASE):
        shutil.rmtree(LOCAL_OUT_BASE)
    os.makedirs(LOCAL_OUT_BASE, exist_ok=True)

    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table,
        root_path=LOCAL_OUT_BASE,
        partition_cols=['ingest_date'],
        compression='snappy',
        existing_data_behavior='overwrite_or_ignore',
    )

    context['ti'].xcom_push(key='ingest_date', value=ingest_date)


def _upload_to_minio(**context):
    ingest_date = context['ti'].xcom_pull(key='ingest_date')
    if not ingest_date:
        raise AirflowFailException("ingest_date xcom missing; transform task likely failed.")

    s3 = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
        config=Config(signature_version='s3v4'),
    )

    uploaded = 0
    prefix = f"{TARGET_PREFIX}/ingest_date={ingest_date}"
    part_dir = os.path.join(LOCAL_OUT_BASE, f"ingest_date={ingest_date}")
    if not os.path.exists(part_dir):
        raise AirflowFailException(f"Partition directory not found: {part_dir}")

    for fname in os.listdir(part_dir):
        if not fname.endswith('.parquet'):
            continue
        full = os.path.join(part_dir, fname)
        key = f"{prefix}/{fname}"
        s3.upload_file(full, MINIO_BUCKET, key)
        uploaded += 1

    if uploaded == 0:
        raise AirflowFailException("No Parquet files were uploaded (0 files found).")

    print(f"Uploaded {uploaded} parquet file(s) to s3://{MINIO_BUCKET}/{prefix}")


default_args = {"owner": "data-eng", "retries": 0}

with DAG(
    dag_id="customer_care_emails_to_minio",
    description="Validate CSV, transform, write Parquet, upload to MinIO (partitioned by ingest_date).",
    start_date=dt.datetime(2025, 9, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "minio", "parquet", "csv"],
) as dag:

    file_check = PythonOperator(
        task_id="file_check",
        python_callable=_assert_file_exists,
    )

    schema_validation = PythonOperator(
        task_id="schema_validation",
        python_callable=_validate_schema,
    )

    transform_to_parquet = PythonOperator(
        task_id="transform_to_parquet",
        python_callable=_transform_and_write_parquet,
    )

    upload_minio = PythonOperator(
        task_id="upload_to_minio",
        python_callable=_upload_to_minio,
    )

    file_check >> schema_validation >> transform_to_parquet >> upload_minio
