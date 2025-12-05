# Customer Care Email to MinIO Parquet Pipeline

Airflow-based pipeline that ingests customer care email CSV files, validates against schema, transforms data, and loads to MinIO as partitioned Parquet.

## Quick Start

```bash
# Setup environment
cp .env.sample .env
echo "AIRFLOW_UID=$(id -u)" >> .env  # Linux/Mac only

# Start services
docker compose up --build -d

# Access UIs
# Airflow: http://localhost:8080 (admin/admin)
# MinIO: http://localhost:9001 (minioadmin/minioadmin)
```

## Pipeline Flow

```
Customer Care Email CSV → Schema Validation → Transform → Parquet → MinIO
```

1. **File Check**: Verify CSV exists (~20K email records)
2. **Schema Validation**: Check against YAML schema contract
3. **Transform**: Clean data, add partition columns (email_year/month)
4. **Upload**: Store as partitioned Parquet in MinIO

## Data Structure

- **Source**: Customer support email conversations
- **Records**: ~20,490 email exchanges
- **Columns**: subject, sender, receiver, timestamp, message_body, thread_id, ratings, etc.
- **Partitioning**: By email year and month

## Configuration

- **Schema**: `ingestion/customer_care_emails/config/schema_expected.yaml`
- **Environment**: `.env` file
- **DAG**: `dags/ingestion/customer_data_to_minio.py`

## Monitoring

```bash
# View logs
docker compose logs airflow-scheduler

# Stop services  
docker compose down
```
