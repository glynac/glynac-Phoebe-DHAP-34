# Customer Care Emails Pipeline

## Quick Start

### Prerequisites
- Docker + Docker Compose
- 4GB+ RAM available

### Setup
```bash
# Copy environment template
cp .env.example .env

# Start services
docker compose up -d

# Access Airflow UI: http://localhost:8080
# Username: airflow, Password: airflow
```

### Usage
1. Trigger `customer_care_emails_ingest` DAG in Airflow UI
2. Monitor pipeline execution in Graph view
3. Data loads to `postgres-data` service on port 5433

### Cleanup
```bash
docker compose down -v
```

## Pipeline Overview
- **Source**: Hugging Face dataset `rtweera/customer_care_emails`
- **Local File**: `extraction/customer_data/sample_data/customer_care_emails.csv`
- **Target**: PostgreSQL table `public.customer_care_emails`
- **Process**: File Check → Schema Validation → Transform → Load
- **Business Rule**: Skips records with `email_status='completed'`

## Dataset Details
- **Columns**: 13 fields including subject, sender, receiver, timestamp, message_body, thread_id, email_types, email_status, email_criticality, product_types, agent ratings, and customer satisfaction
- **Total Records**: 2,259 records (complete dataset)
- **Processing**: ~930 records to be processed (ongoing status), 1,329 skipped (completed status)
- **File Size**: 1.4MB
- **License**: GPL 3.0
