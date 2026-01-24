"""
Customer Care Emails Ingestion DAG
==================================

This DAG ingests customer care emails from Hugging Face dataset, validates the schema,
applies transformations, and loads the data into PostgreSQL.

Author: Paras Ningune
Date: 2025-11-25
"""

from datetime import datetime, timedelta
import os
import pandas as pd
import yaml
import psycopg2
import json
from sqlalchemy import create_engine
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook


# Default arguments
default_args = {
    'owner': 'paras.ningune',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'customer_care_emails_ingest',
    default_args=default_args,
    description='Ingest customer care emails from Hugging Face dataset to PostgreSQL',
    schedule_interval='@daily',
    catchup=False,
    tags=['data-ingestion', 'csv', 'postgresql', 'customer-care-emails', 'huggingface'],
)

# Configuration paths
BASE_DIR = Path('/opt/airflow/extraction/customer_data')
CSV_PATH = BASE_DIR / 'sample_data' / 'customer_care_emails.csv'
SCHEMA_PATH = BASE_DIR / 'config' / 'schema_expected.yaml'
DDL_PATH = BASE_DIR / 'config' / 'create_table.sql'


def check_file_exists(**context):
    """Check if the CSV file exists."""
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV file not found at {CSV_PATH}")
    
    print(f"✓ CSV file found at {CSV_PATH}")
    file_size = CSV_PATH.stat().st_size
    print(f"✓ File size: {file_size} bytes")
    
    return str(CSV_PATH)


def validate_schema(**context):
    """Validate CSV schema against expected schema."""
    # Load expected schema
    with open(SCHEMA_PATH, 'r') as f:
        expected_schema = yaml.safe_load(f)
    
    # Load CSV and check schema
    df = pd.read_csv(CSV_PATH)
    
    print(f"✓ CSV loaded successfully with {len(df)} rows")
    print(f"✓ CSV columns: {list(df.columns)}")
    
    # Check columns match
    expected_columns = [col['name'] for col in expected_schema['columns']]
    csv_columns = list(df.columns)
    
    if set(expected_columns) != set(csv_columns):
        missing_cols = set(expected_columns) - set(csv_columns)
        extra_cols = set(csv_columns) - set(expected_columns)
        
        error_msg = "Schema validation failed!\n"
        if missing_cols:
            error_msg += f"Missing columns: {missing_cols}\n"
        if extra_cols:
            error_msg += f"Unexpected columns: {extra_cols}\n"
        
        raise ValueError(error_msg)
    
    # Check for required non-nullable fields
    for col_def in expected_schema['columns']:
        if not col_def['nullable']:
            null_count = df[col_def['name']].isnull().sum()
            if null_count > 0:
                print(f"⚠ Warning: {null_count} null values found in required column '{col_def['name']}'")
    
    print("✓ Schema validation passed")
    return True


def transform_data(**context):
    """Apply basic transformations to the data."""
    df = pd.read_csv(CSV_PATH)
    
    print(f"Starting transformation with {len(df)} rows")
    
    # Skip rows where email_status is 'completed'
    initial_count = len(df)
    df = df[df['email_status'] != 'completed']
    skipped_count = initial_count - len(df)
    print(f"✓ Skipped {skipped_count} rows with email_status='completed'")
    
    # Basic transformations
    # Strip whitespace from text fields
    text_columns = ['subject', 'sender', 'receiver', 'email_status', 'email_criticality', 'agent_effectivity', 'agent_efficiency']
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    
    # Handle JSON columns
    if 'email_types' in df.columns:
        df['email_types'] = df['email_types'].apply(
            lambda x: x if pd.isna(x) or x == '' else x
        )
    
    if 'product_types' in df.columns:
        df['product_types'] = df['product_types'].apply(
            lambda x: x if pd.isna(x) or x == '' else x
        )
    
    # Validate email format (basic check)
    invalid_senders = df[~df['sender'].str.contains('@', na=False)]
    if len(invalid_senders) > 0:
        print(f"⚠ Warning: {len(invalid_senders)} rows with invalid sender email format")
    
    invalid_receivers = df[~df['receiver'].str.contains('@', na=False)]
    if len(invalid_receivers) > 0:
        print(f"⚠ Warning: {len(invalid_receivers)} rows with invalid receiver email format")
    
    # Convert timestamp column
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    
    # Handle customer satisfaction scores (ensure they're numeric)
    if 'customer_satisfaction' in df.columns:
        df['customer_satisfaction'] = pd.to_numeric(df['customer_satisfaction'], errors='coerce')
    
    print(f"✓ Transformation completed. Final dataset: {len(df)} rows")
    
    # Save transformed data
    transformed_path = BASE_DIR / 'sample_data' / 'customer_care_emails_transformed.csv'
    df.to_csv(transformed_path, index=False)
    
    return str(transformed_path)


def create_table(**context):
    """Create the target table if it doesn't exist."""
    # Read DDL
    with open(DDL_PATH, 'r') as f:
        ddl_sql = f.read()
    
    # Get PostgreSQL connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_data')
    
    # Execute DDL
    pg_hook.run(ddl_sql)
    print("✓ Table created or already exists")
    
    return True


def load_data(**context):
    """Load transformed data into PostgreSQL."""
    # Get the transformed file path from previous task
    transformed_path = context['task_instance'].xcom_pull(task_ids='transform_data')
    
    df = pd.read_csv(transformed_path)
    print(f"Loading {len(df)} rows to PostgreSQL")
    
    # Get connection parameters from environment or Airflow Variables
    conn_params = {
        'host': os.getenv('PG_HOST', 'postgres-data'),
        'port': os.getenv('PG_PORT', '5432'),
        'database': os.getenv('PG_DB', 'airflow_data'),
        'user': os.getenv('PG_USER', 'pipeline_user'),
        'password': os.getenv('PG_PASSWORD', 'pipeline_password'),
    }
    
    # Create SQLAlchemy engine
    engine = create_engine(
        f"postgresql+psycopg2://{conn_params['user']}:{conn_params['password']}@"
        f"{conn_params['host']}:{conn_params['port']}/{conn_params['database']}"
    )
    
    # Load data using pandas to_sql with conflict resolution
    try:
        # Use replace method to handle duplicates
        df.to_sql('customer_care_emails', engine, schema='public', 
                 if_exists='append', index=False, method='multi')
        print(f"✓ Successfully loaded {len(df)} rows to customer_care_emails table")
        
    except Exception as e:
        print(f"Error during load: {str(e)}")
        # For duplicate key errors, try individual inserts with ignore
        if 'duplicate key' in str(e).lower():
            success_count = 0
            for _, row in df.iterrows():
                try:
                    row_df = pd.DataFrame([row])
                    row_df.to_sql('customer_care_emails', engine, schema='public',
                                if_exists='append', index=False)
                    success_count += 1
                except Exception as row_error:
                    print(f"⚠ Skipped row {row['thread_id']}: {row_error}")
                    continue
            
            print(f"✓ Loaded {success_count} out of {len(df)} rows (skipped duplicates)")
        else:
            raise e
    
    return len(df)


# Define tasks
file_check_task = PythonOperator(
    task_id='check_file_exists',
    python_callable=check_file_exists,
    dag=dag,
)

schema_validation_task = PythonOperator(
    task_id='validate_schema',
    python_callable=validate_schema,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Define task dependencies
file_check_task >> schema_validation_task >> transform_task
transform_task >> create_table_task >> load_task
