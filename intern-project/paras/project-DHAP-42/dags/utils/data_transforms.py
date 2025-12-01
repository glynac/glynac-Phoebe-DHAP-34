"""
Data transformation utilities for the pipeline
"""
import pandas as pd
from typing import Dict, Any
import yaml


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply basic data cleaning operations
    """
    df_clean = df.copy()
    
    # Strip whitespace from string columns
    string_columns = df_clean.select_dtypes(include=['object', 'string']).columns
    for col in string_columns:
        df_clean[col] = df_clean[col].astype(str).str.strip()
        # Replace empty strings with NaN
        df_clean[col] = df_clean[col].replace('', None)
    
    # Remove completely empty rows
    df_clean = df_clean.dropna(how='all')
    
    return df_clean


def add_partition_columns(df: pd.DataFrame, schema: Dict[str, Any]) -> pd.DataFrame:
    """
    Add partition columns based on schema configuration
    """
    df_partitioned = df.copy()
    
    # Check if partitioning is enabled
    partitioning = schema.get('partitioning', {})
    if not partitioning.get('enabled', False):
        return df_partitioned
    
    partition_columns = partitioning.get('columns', [])
    
    for partition_col in partition_columns:
        if partition_col == 'email_year':
            if 'timestamp' in df_partitioned.columns:
                # Parse timestamp and extract year
                df_partitioned['email_year'] = pd.to_datetime(
                    df_partitioned['timestamp'], errors='coerce'
                ).dt.year
        
        elif partition_col == 'email_month':
            if 'timestamp' in df_partitioned.columns:
                # Parse timestamp and extract month
                df_partitioned['email_month'] = pd.to_datetime(
                    df_partitioned['timestamp'], errors='coerce'
                ).dt.month
    
    return df_partitioned


def validate_business_rules(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Apply business rule validations
    """
    validation_results = {
        'valid': True,
        'warnings': [],
        'row_count_before': len(df),
        'row_count_after': len(df)
    }
    
    # Check for duplicate email threads
    if 'thread_id' in df.columns:
        duplicate_threads = df['thread_id'].duplicated().sum()
        if duplicate_threads > 0:
            validation_results['warnings'].append(
                f"Found {duplicate_threads} duplicate thread IDs (expected for email conversations)"
            )
    
    # Check for valid email addresses
    if 'sender' in df.columns:
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        invalid_senders = ~df['sender'].str.match(email_pattern, na=False)
        invalid_count = invalid_senders.sum()
        if invalid_count > 0:
            validation_results['warnings'].append(
                f"Found {invalid_count} records with invalid sender email formats"
            )
    
    # Check for future timestamps
    if 'timestamp' in df.columns:
        try:
            timestamps = pd.to_datetime(df['timestamp'], errors='coerce')
            future_timestamps = (timestamps > pd.Timestamp.now()).sum()
            if future_timestamps > 0:
                validation_results['warnings'].append(
                    f"Found {future_timestamps} records with future timestamps"
                )
        except Exception:
            validation_results['warnings'].append("Could not validate timestamp format")
    
    # Check customer satisfaction scores
    if 'customer_satisfaction' in df.columns:
        try:
            satisfaction_scores = pd.to_numeric(df['customer_satisfaction'], errors='coerce')
            invalid_scores = ((satisfaction_scores < -1) | (satisfaction_scores > 1)).sum()
            if invalid_scores > 0:
                validation_results['warnings'].append(
                    f"Found {invalid_scores} records with invalid satisfaction scores (should be -1 to 1)"
                )
        except Exception:
            validation_results['warnings'].append("Could not validate customer satisfaction scores")
    
    return validation_results


def transform_data(df: pd.DataFrame, schema_path: str) -> pd.DataFrame:
    """
    Apply all transformations to the DataFrame
    """
    # Load schema
    with open(schema_path, 'r') as f:
        schema = yaml.safe_load(f)
    
    # Apply transformations
    df_transformed = clean_dataframe(df)
    df_transformed = add_partition_columns(df_transformed, schema)
    
    return df_transformed
