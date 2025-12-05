"""
Customer Care Email Ingestion Pipeline
CSV → Schema Validation → Transform → Parquet → MinIO

This DAG ingests customer care email data from a local CSV file, validates it against
a predefined schema, transforms the data, and loads it into MinIO as 
partitioned Parquet files.
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException

import pandas as pd
import yaml

# Add utils to path
sys.path.append('/opt/airflow/dags/utils')

from schema_validation import SchemaValidator, convert_column_types
from minio_utils import get_minio_client_from_env
from data_transforms import transform_data, validate_business_rules


# DAG Configuration
DAG_ID = "customer_care_emails_to_minio"
DATASET_NAME = os.getenv('DATASET_NAME', 'customer_care_emails')
SOURCE_FILE_PATH = os.getenv('SOURCE_FILE_PATH', 'dags/extraction/sample.csv')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'data-lake')
TARGET_BUCKET_PATH = os.getenv('TARGET_BUCKET_PATH', 'bronze/customer_care_emails/')

# File paths
SCHEMA_PATH = f'/opt/airflow/ingestion/{DATASET_NAME}/config/schema_expected.yaml'
MANIFEST_PATH = f'/opt/airflow/ingestion/{DATASET_NAME}/MANIFEST.md'


# Default DAG arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def check_file_exists(**context):
    """Check if the source CSV file exists"""
    file_path = f'/opt/airflow/{SOURCE_FILE_PATH}'
    
    if not os.path.exists(file_path):
        raise AirflowException(f"Source file not found: {file_path}")
    
    file_size = os.path.getsize(file_path)
    print(f"Source file found: {file_path} (size: {file_size} bytes)")
    
    # Store file info for downstream tasks
    context['task_instance'].xcom_push(key='source_file_path', value=file_path)
    context['task_instance'].xcom_push(key='source_file_size', value=file_size)


def load_and_validate_schema(**context):
    """Load CSV data and validate against schema"""
    # Get file path from previous task
    file_path = context['task_instance'].xcom_pull(key='source_file_path')
    
    # Load CSV data
    try:
        df = pd.read_csv(file_path)
        print(f"Loaded CSV with {len(df)} rows and {len(df.columns)} columns")
        print(f"Columns: {list(df.columns)}")
    except Exception as e:
        raise AirflowException(f"Error loading CSV file: {e}")
    
    # Validate against schema
    try:
        validator = SchemaValidator(SCHEMA_PATH)
        validation_results = validator.validate_dataframe(df)
        
        print(f"Schema validation results:")
        print(f"- Valid: {validation_results['valid']}")
        print(f"- Errors: {validation_results['errors']}")
        print(f"- Warnings: {validation_results['warnings']}")
        
        # Fail if validation errors exist
        if not validation_results['valid']:
            raise AirflowException(f"Schema validation failed: {validation_results['errors']}")
        
        # Store validation results
        context['task_instance'].xcom_push(key='validation_results', value=validation_results)
        context['task_instance'].xcom_push(key='raw_data_path', value=file_path)
        
    except FileNotFoundError as e:
        raise AirflowException(f"Schema file not found: {e}")
    except Exception as e:
        raise AirflowException(f"Schema validation error: {e}")


def transform_data_task(**context):
    """Transform and clean the data"""
    # Get file path from previous task
    file_path = context['task_instance'].xcom_pull(key='raw_data_path')
    
    # Load data
    df = pd.read_csv(file_path)
    print(f"Starting transformation on {len(df)} rows")
    
    # Apply transformations
    try:
        df_transformed = transform_data(df, SCHEMA_PATH)
        
        # Apply business rule validation
        business_validation = validate_business_rules(df_transformed)
        print(f"Business validation results: {business_validation}")
        
        # Convert to proper types
        with open(SCHEMA_PATH, 'r') as f:
            schema = yaml.safe_load(f)
        df_typed = convert_column_types(df_transformed, schema)
        
        print(f"Transformation completed. Final shape: {df_typed.shape}")
        print(f"Final columns: {list(df_typed.columns)}")
        
        # Save transformed data temporarily
        temp_file = f'/tmp/transformed_{DATASET_NAME}.parquet'
        df_typed.to_parquet(temp_file, compression='snappy', index=False)
        
        # Store temp file path for next task
        context['task_instance'].xcom_push(key='transformed_data_path', value=temp_file)
        context['task_instance'].xcom_push(key='transformed_row_count', value=len(df_typed))
        context['task_instance'].xcom_push(key='business_validation', value=business_validation)
        
    except Exception as e:
        raise AirflowException(f"Data transformation error: {e}")


def upload_to_minio(**context):
    """Upload transformed data to MinIO as partitioned Parquet"""
    # Get transformed data path
    temp_file = context['task_instance'].xcom_pull(key='transformed_data_path')
    
    # Load transformed data
    df = pd.read_parquet(temp_file)
    print(f"Loading {len(df)} rows for MinIO upload")
    
    try:
        # Initialize MinIO client
        minio_client = get_minio_client_from_env()
        
        # Ensure bucket exists
        if not minio_client.ensure_bucket_exists(MINIO_BUCKET):
            raise AirflowException(f"Failed to create/access bucket: {MINIO_BUCKET}")
        
        # Generate timestamped path
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_path = f"{TARGET_BUCKET_PATH}{timestamp}"
        
        # Upload as partitioned Parquet
        partition_cols = ['email_year'] if 'email_year' in df.columns else []
        
        success = minio_client.upload_partitioned_parquet(
            df=df,
            bucket_name=MINIO_BUCKET,
            base_path=base_path,
            partition_cols=partition_cols
        )
        
        if not success:
            raise AirflowException("Failed to upload data to MinIO")
        
        print(f"Successfully uploaded data to MinIO: {MINIO_BUCKET}/{base_path}")
        
        # Store upload info
        context['task_instance'].xcom_push(key='minio_path', value=f"{MINIO_BUCKET}/{base_path}")
        context['task_instance'].xcom_push(key='upload_timestamp', value=timestamp)
        
        # Clean up temp file
        if os.path.exists(temp_file):
            os.remove(temp_file)
            
    except Exception as e:
        raise AirflowException(f"MinIO upload error: {e}")


def generate_summary_report(**context):
    """Generate a summary report of the pipeline execution"""
    # Gather information from previous tasks
    validation_results = context['task_instance'].xcom_pull(key='validation_results')
    business_validation = context['task_instance'].xcom_pull(key='business_validation')
    transformed_row_count = context['task_instance'].xcom_pull(key='transformed_row_count')
    minio_path = context['task_instance'].xcom_pull(key='minio_path')
    upload_timestamp = context['task_instance'].xcom_pull(key='upload_timestamp')
    source_file_size = context['task_instance'].xcom_pull(key='source_file_size')
    
    # Generate summary
    summary = {
        'pipeline_execution': {
            'dataset': DATASET_NAME,
            'execution_date': context['ds'],
            'execution_timestamp': upload_timestamp,
            'success': True
        },
        'source_data': {
            'file_path': SOURCE_FILE_PATH,
            'file_size_bytes': source_file_size,
            'original_row_count': validation_results.get('row_count', 0)
        },
        'validation': {
            'schema_validation': validation_results,
            'business_validation': business_validation
        },
        'transformation': {
            'final_row_count': transformed_row_count,
            'columns_added': ['email_year', 'email_month']
        },
        'output': {
            'minio_bucket': MINIO_BUCKET,
            'minio_path': minio_path,
            'format': 'parquet',
            'compression': 'snappy',
            'partitioned': True
        }
    }
    
    print("="*50)
    print("PIPELINE EXECUTION SUMMARY")
    print("="*50)
    print(f"Dataset: {summary['pipeline_execution']['dataset']}")
    print(f"Execution Date: {summary['pipeline_execution']['execution_date']}")
    print(f"Source Rows: {summary['source_data']['original_row_count']}")
    print(f"Final Rows: {summary['transformation']['final_row_count']}")
    print(f"Schema Valid: {summary['validation']['schema_validation']['valid']}")
    print(f"Output Location: {summary['output']['minio_path']}")
    print("="*50)
    
    # Store complete summary
    context['task_instance'].xcom_push(key='pipeline_summary', value=summary)


# Create the DAG
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='Ingest customer care email CSV to MinIO as partitioned Parquet',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['data-ingestion', 'csv', 'minio', 'parquet', 'customer-care', 'emails'],
) as dag:
    
    # Task 1: File existence check
    check_source_file = PythonOperator(
        task_id='check_source_file_exists',
        python_callable=check_file_exists,
        doc_md=f"""
        ### File Check Task
        
        Verifies that the source CSV file exists at: `{SOURCE_FILE_PATH}`
        
        **Outputs:**
        - source_file_path: Absolute path to the source file
        - source_file_size: Size of the source file in bytes
        """,
    )
    
    # Task 2: Schema validation task group
    with TaskGroup(group_id='validation_group') as validation_tasks:
        
        validate_schema = PythonOperator(
            task_id='validate_schema_contract',
            python_callable=load_and_validate_schema,
            doc_md=f"""
            ### Schema Validation Task
            
            Loads the CSV data and validates it against the schema definition at: `{SCHEMA_PATH}`
            
            **Validation Checks:**
            - Column presence and naming
            - Data type compatibility
            - Null value constraints
            - Value range constraints
            - Pattern matching for string fields
            
            **Outputs:**
            - validation_results: Detailed validation results
            - raw_data_path: Path to the raw CSV file
            """,
        )
    
    # Task 3: Data transformation task group
    with TaskGroup(group_id='transformation_group') as transform_tasks:
        
        transform_data_op = PythonOperator(
            task_id='transform_and_clean_data',
            python_callable=transform_data_task,
            doc_md="""
            ### Data Transformation Task
            
            Applies data cleaning and transformation operations:
            
            **Transformations:**
            - Strip whitespace from string fields
            - Convert data types per schema
            - Add partition columns (year/month from registration_date)
            - Apply business rule validations
            
            **Outputs:**
            - transformed_data_path: Path to transformed Parquet file
            - transformed_row_count: Number of rows after transformation
            - business_validation: Results of business rule validation
            """,
        )
    
    # Task 4: MinIO upload task group  
    with TaskGroup(group_id='load_group') as load_tasks:
        
        upload_parquet = PythonOperator(
            task_id='upload_to_minio',
            python_callable=upload_to_minio,
            doc_md=f"""
            ### MinIO Upload Task
            
            Uploads the transformed data to MinIO as partitioned Parquet files.
            
            **Configuration:**
            - Bucket: `{MINIO_BUCKET}`
            - Base Path: `{TARGET_BUCKET_PATH}`
            - Format: Parquet with Snappy compression
            - Partitioning: By registration_year (if available)
            
            **Outputs:**
            - minio_path: Full path to uploaded data in MinIO
            - upload_timestamp: Timestamp of the upload operation
            """,
        )
    
    # Task 5: Summary report
    summary_report = PythonOperator(
        task_id='generate_summary_report',
        python_callable=generate_summary_report,
        doc_md="""
        ### Summary Report Task
        
        Generates a comprehensive summary of the pipeline execution including:
        - Source data statistics
        - Validation results
        - Transformation metrics
        - Output location and format
        
        **Outputs:**
        - pipeline_summary: Complete execution summary
        """,
    )
    
    # Define task dependencies
    check_source_file >> validation_tasks >> transform_tasks >> load_tasks >> summary_report
