"""
MinIO utilities for data pipeline operations
"""
import os
import io
from typing import Optional, List
from minio import Minio
from minio.error import S3Error
import pandas as pd


class MinIOClient:
    """MinIO client wrapper for data operations"""
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str, 
                 secure: bool = False):
        """Initialize MinIO client"""
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        
    def ensure_bucket_exists(self, bucket_name: str) -> bool:
        """Create bucket if it doesn't exist"""
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                print(f"Created bucket: {bucket_name}")
            return True
        except S3Error as e:
            print(f"Error creating bucket {bucket_name}: {e}")
            return False
    
    def upload_parquet(self, df: pd.DataFrame, bucket_name: str, 
                      object_path: str) -> bool:
        """Upload DataFrame as Parquet to MinIO"""
        try:
            # Convert DataFrame to Parquet bytes
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, compression='snappy', index=False)
            parquet_buffer.seek(0)
            
            # Upload to MinIO
            self.client.put_object(
                bucket_name=bucket_name,
                object_name=object_path,
                data=parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type='application/parquet'
            )
            
            print(f"Successfully uploaded {object_path} to bucket {bucket_name}")
            return True
            
        except Exception as e:
            print(f"Error uploading to MinIO: {e}")
            return False
    
    def upload_partitioned_parquet(self, df: pd.DataFrame, bucket_name: str,
                                  base_path: str, partition_cols: List[str]) -> bool:
        """Upload DataFrame as partitioned Parquet files"""
        try:
            if not partition_cols:
                # No partitioning, upload as single file
                object_path = f"{base_path}/data.parquet"
                return self.upload_parquet(df, bucket_name, object_path)
            
            # Group by partition columns
            partition_col = partition_cols[0]  # Use first partition column
            
            if partition_col not in df.columns:
                print(f"Warning: Partition column '{partition_col}' not found in DataFrame")
                object_path = f"{base_path}/data.parquet"
                return self.upload_parquet(df, bucket_name, object_path)
            
            # Create partitions
            success = True
            for partition_value in df[partition_col].unique():
                if pd.isna(partition_value):
                    continue
                    
                partition_df = df[df[partition_col] == partition_value]
                partition_path = f"{base_path}/{partition_col}={partition_value}/data.parquet"
                
                if not self.upload_parquet(partition_df, bucket_name, partition_path):
                    success = False
            
            return success
            
        except Exception as e:
            print(f"Error uploading partitioned data: {e}")
            return False
    
    def list_objects(self, bucket_name: str, prefix: str = "") -> List[str]:
        """List objects in bucket with optional prefix"""
        try:
            objects = self.client.list_objects(bucket_name, prefix=prefix, recursive=True)
            return [obj.object_name for obj in objects]
        except S3Error as e:
            print(f"Error listing objects: {e}")
            return []
    
    def download_object(self, bucket_name: str, object_name: str, 
                       local_path: str) -> bool:
        """Download object from MinIO to local file"""
        try:
            self.client.fget_object(bucket_name, object_name, local_path)
            print(f"Downloaded {object_name} to {local_path}")
            return True
        except S3Error as e:
            print(f"Error downloading object: {e}")
            return False


def get_minio_client_from_env() -> MinIOClient:
    """Create MinIO client from environment variables"""
    endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
    access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
    secure = os.getenv('MINIO_SECURE', 'false').lower() == 'true'
    
    return MinIOClient(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure
    )
