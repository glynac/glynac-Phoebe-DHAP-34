"""
Gold Layer: Customer Lifetime Value (CLV) Calculation
Uses SQL preprocessing + Python for CLV modeling
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict


def calculate_clv(df: pd.DataFrame, execution_date: str) -> pd.DataFrame:
    """
    Calculate Customer Lifetime Value using RFM-based approach
    
    Args:
        df: Preprocessed customer data from SQL query
        execution_date: Execution date
    
    Returns:
        DataFrame with CLV scores
    """
    # Avoid division by zero
    df['customer_lifespan_days'] = df['customer_lifespan_days'].replace(0, 1)
    
    # Calculate purchase rate
    df['purchase_rate'] = df['frequency'] / (df['customer_lifespan_days'] / 365.0)
    
    # Calculate average order value
    df['avg_order_value'] = df['total_revenue'] / df['frequency']
    
    # Simple CLV model: 
    # CLV = (Average Order Value) × (Purchase Frequency) × (Customer Lifespan in years) × (Profit Margin)
    # Assuming 20% profit margin
    profit_margin = 0.20
    average_lifespan_years = 3  # Assume 3-year customer lifetime
    
    df['predicted_clv'] = (
        df['avg_order_value'] * 
        df['purchase_rate'] * 
        average_lifespan_years * 
        profit_margin
    ).round(2)
    
    # RFM Scoring (1-5 scale)
    df['recency_score'] = pd.qcut(
        df['recency_days'], 
        q=5, 
        labels=[5, 4, 3, 2, 1],  # Lower recency = higher score
        duplicates='drop'
    ).astype(int)
    
    df['frequency_score'] = pd.qcut(
        df['frequency'], 
        q=5, 
        labels=[1, 2, 3, 4, 5],  # Higher frequency = higher score
        duplicates='drop'
    ).astype(int)
    
    df['monetary_score'] = pd.qcut(
        df['monetary'], 
        q=5, 
        labels=[1, 2, 3, 4, 5],  # Higher monetary = higher score
        duplicates='drop'
    ).astype(int)
    
    # Overall RFM score
    df['rfm_score'] = (
        df['recency_score'] + 
        df['frequency_score'] + 
        df['monetary_score']
    )
    
    # Customer segment
    df['customer_segment'] = df['rfm_score'].apply(classify_customer)
    
    # Add metadata
    df['_calculated_at'] = datetime.now()
    df['_execution_date'] = execution_date
    
    return df


def classify_customer(rfm_score: int) -> str:
    """Classify customer based on RFM score"""
    if rfm_score >= 13:
        return 'Champions'
    elif rfm_score >= 10:
        return 'Loyal Customers'
    elif rfm_score >= 7:
        return 'Potential Loyalists'
    elif rfm_score >= 5:
        return 'At Risk'
    else:
        return 'Lost'


def transform(clickhouse_client, sql_result: pd.DataFrame, execution_date: str, **context) -> Dict:
    """
    Main transformation function
    
    Args:
        clickhouse_client: ClickHouse client
        sql_result: Result from SQL preprocessing
        execution_date: Execution date
        **context: Airflow context
    
    Returns:
        Dict with status and data
    """
    print(f"Starting CLV calculation for {len(sql_result)} customers")
    
    if sql_result.empty:
        return {
            'status': 'success',
            'rows_processed': 0,
            'message': 'No customers to process'
        }
    
    # Calculate CLV
    result_df = calculate_clv(sql_result, execution_date)
    
    print(f"Calculated CLV for {len(result_df)} customers")
    print(f"Segment distribution:\n{result_df['customer_segment'].value_counts()}")
    
    return {
        'status': 'success',
        'rows_processed': len(result_df),
        'data': result_df,
        'execution_date': execution_date
    }