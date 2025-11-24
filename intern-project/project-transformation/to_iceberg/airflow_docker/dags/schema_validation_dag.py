from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
import os

AZURE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = "rawdata"

EXPECTED_SCHEMAS = {
    'user_referrals.csv': {
        'referral_at': str,
        'referral_id': str,
        'referee_id': str,
        'referee_name': str,
        'referee_phone': str,
        'referral_reward_id': str,
        'referral_source': str,
        'referrer_id': str,
        'transaction_id': str,
        'updated_at': str,
        'user_referral_status_id': str,
    },
    'user_logs.csv': {
        'id': str,
        'user_id': str,
        'name': str,
        'phone_number': str,
        'homeclub': str,
        'timezone_homeclub': str,
        'membership_expired_date': str,
        'is_deleted': str,
    },
    'user_referral_logs.csv': {
        'id': str,
        'user_referral_id': str,
        'source_transaction_id': str,
        'created_at': str,
        'is_reward_granted': str,
    },
    'referral_rewards.csv': {
        'id': str,
        'reward_value': str,
        'created_at': str,
        'reward_type': str,
    },
    'user_referral_statuses.csv': {
        'id': str,
        'description': str,
        'created_at': str,
    },
    'paid_transactions.csv': {
        'transaction_id': str,
        'transaction_status': str,
        'transaction_at': str,
        'transaction_location': str,
        'timezone_transaction': str,
        'transaction_type': str,
    },
    'lead_log.csv': {
        'id': str,
        'lead_id': str,
        'source_category': str,
        'created_at': str,
        'preferred_location': str,
        'timezone_location': str,
        'current_status': str,
    }
}

def validate_schema():
    if not AZURE_CONN_STR:
        raise AirflowException("AZURE_STORAGE_CONNECTION_STRING is not set in environment.")
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    for filename, expected_schema in EXPECTED_SCHEMAS.items():
        try:
            blob_client = container_client.get_blob_client(filename)
            blob_data = blob_client.download_blob().readall()
            df = pd.read_csv(io.BytesIO(blob_data))
        except Exception as e:
            raise AirflowException(f"Error loading {filename} from Azure Blob Storage: {e}")
        
        missing_cols = [col for col in expected_schema if col not in df.columns]
        if missing_cols:
            raise AirflowException(f"Missing columns in {filename}: {missing_cols}")
        
        for col, dtype in expected_schema.items():
            try:
                df[col].astype(dtype)
            except Exception:
                raise AirflowException(f"Column '{col}' in {filename} cannot be coerced to {dtype}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='schema_validation_dag',
    default_args=default_args,
    description='Validate schema of raw CSVs in Azure before further processing',
    schedule_interval=None,
    catchup=False,
    tags=['validation', 'azure', 'iceberg'],
) as dag:
    validate_schema_task = PythonOperator(
        task_id='validate_schema',
        python_callable=validate_schema
    )
