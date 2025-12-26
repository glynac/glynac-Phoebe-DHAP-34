"""
Gold Layer: Revenue Metrics
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List
import numpy as np


def calculate_revenue_metrics(df: pd.DataFrame, execution_date: str) -> pd.DataFrame:
    """
    Calculate advanced revenue metrics that are complex to do in SQL
    
    Args:
        df: Input DataFrame from Silver layer
        execution_date: Airflow execution date (YYYY-MM-DD)
    
    Returns:
        DataFrame with calculated metrics
    """
    # Convert execution_date to datetime
    exec_date = pd.to_datetime(execution_date)
    
    # Filter to execution date
    df['processing_date'] = pd.to_datetime(df['processing_date'])
    df = df[df['processing_date'] == exec_date]
    
    # Group by organization and date
    grouped = df.groupby(['glynac_organization_id', 'processing_date'])
    
    # Calculate metrics
    result = grouped.agg({
        'revenue': [
            'sum',           # Total revenue
            'mean',          # Average revenue
            'std',           # Standard deviation
            'count'          # Number of transactions
        ],
        'customer_id': 'nunique'  # Unique customers
    }).reset_index()
    
    # Flatten column names
    result.columns = [
        'organization_id',
        'metric_date',
        'total_revenue',
        'avg_revenue',
        'std_revenue',
        'transaction_count',
        'unique_customers'
    ]
    
    # Calculate additional metrics
    result['revenue_per_customer'] = (
        result['total_revenue'] / result['unique_customers']
    ).round(2)
    
    result['revenue_coefficient_of_variation'] = (
        result['std_revenue'] / result['avg_revenue']
    ).round(4)
    
    # Calculate growth rate (compared to yesterday)
    # This is where Python shines - complex window calculations
    result = result.sort_values(['organization_id', 'metric_date'])
    result['prev_day_revenue'] = result.groupby('organization_id')['total_revenue'].shift(1)
    result['revenue_growth_rate'] = (
        (result['total_revenue'] - result['prev_day_revenue']) / result['prev_day_revenue'] * 100
    ).round(2)
    
    # Add metadata
    result['_calculated_at'] = datetime.now()
    result['_execution_date'] = execution_date
    
    # Handle nulls
    result = result.fillna(0)
    
    return result


def validate_output(df: pd.DataFrame) -> bool:
    """
    Validate the output DataFrame
    
    Args:
        df: Output DataFrame
    
    Returns:
        True if valid, raises exception otherwise
    """
    required_columns = [
        'organization_id', 'metric_date', 'total_revenue',
        'avg_revenue', 'transaction_count', 'unique_customers'
    ]
    
    # Check required columns
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    # Check for nulls in key columns
    null_counts = df[required_columns].isnull().sum()
    if null_counts.any():
        raise ValueError(f"Null values found in required columns: {null_counts[null_counts > 0]}")
    
    # Check data types
    if not pd.api.types.is_numeric_dtype(df['total_revenue']):
        raise ValueError("total_revenue must be numeric")
    
    return True


# Required entry point - called by GoldPythonOperator
def transform(clickhouse_client, execution_date: str, **context) -> Dict:
    """
    Main transformation function called by Airflow
    
    Args:
        clickhouse_client: ClickHouse client instance
        execution_date: Airflow execution date
        **context: Airflow context
    
    Returns:
        Dict with status and metrics
    """
    print(f"Starting revenue metrics calculation for {execution_date}")
    
    # Read data from Silver layer
    query = f"""
    SELECT 
        glynac_organization_id,
        processing_date,
        revenue,
        customer_id
    FROM silver.revenue_transactions
    WHERE processing_date = toDate('{execution_date}')
    """
    
    # Execute query and get DataFrame
    result = clickhouse_client.query_dataframe(query)
    df = pd.DataFrame(result)
    
    print(f"Loaded {len(df)} rows from Silver layer")
    
    if df.empty:
        print("Warning: No data found for this date")
        return {
            'status': 'success',
            'rows_processed': 0,
            'message': 'No data for this date'
        }
    
    # Calculate metrics
    output_df = calculate_revenue_metrics(df, execution_date)
    
    # Validate output
    validate_output(output_df)
    
    print(f"Calculated metrics for {len(output_df)} organizations")
    
    # Return results for insertion
    # GoldPythonOperator will handle the INSERT
    return {
        'status': 'success',
        'rows_processed': len(output_df),
        'data': output_df,
        'execution_date': execution_date
    }