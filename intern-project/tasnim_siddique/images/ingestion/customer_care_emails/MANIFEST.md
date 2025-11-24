# Dataset Manifest — customer_care_emails

**Dataset Name:** customer_care_emails  
**Owner:** data-eng  
**Version:** 0.1.0

## Source
- **File Type:** CSV
- **Path:** `dags/extraction/customer_care_emails.csv`
- **Delimiter:** `,`
- **Header Row:** true
- **Encoding:** UTF-8

## Validation
- Enforce `schema_expected.yaml` (columns, types, nullability, primary key).
- **Fail DAG on mismatch:** yes

## Transformations (high level)
- Trim string columns
- Parse dates/timestamps
- Normalize column names to `snake_case`

## Target (MinIO / S3-compatible)
- **Bucket:** `${MINIO_BUCKET}`
- **Base Path (prefix):** `customer_care_emails/` *(override via `${MINIO_PREFIX}` if desired)*
- **File Format:** Parquet (snappy)
- **Partitioning:** `ingest_date=YYYY-MM-DD`
  - Final layout example:
    - `s3a://${MINIO_BUCKET}/customer_care_emails/ingest_date=2025-09-04/part-00000-....parquet`

## Operational Notes
- **Run Env:** Docker Compose (Airflow + MinIO)
- **Credentials:** via `.env` (see `.env.sample`)
- **Repro Steps:** documented in repo README
