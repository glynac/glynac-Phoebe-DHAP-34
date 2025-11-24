# Airflow: CSV → Parquet → MinIO (Local, Dockerized)

This repo contains an Apache Airflow pipeline that ingests a **local CSV**, enforces a **schema contract**, **transforms** the data, writes **partitioned Parquet**, and uploads it to **MinIO** (S3-compatible).

## Overview
```
CSV (local) ──> validate schema ──> transform ──> Parquet ──> MinIO (bronze)
```
- **DAG**: `customer_care_emails_to_minio`
- **Input CSV**: `dags/extraction/customer_care_emails.csv` 
- **Schema**: `ingestion/customer_care_emails/config/schema_expected.yaml`
- **Output path in MinIO**: `s3://$MINIO_BUCKET/customer_care_emails/ingest_date=2025-09-06/*.parquet`

## Repo Structure (expected)
```
.
├─ docker-compose.yml
├─ .env                         # runtime env (copy from .env.sample)
├─ requirements.txt
├─ dags/
│  ├─ ingestion/
│  │  └─ customer_care_emails_to_minio.py
│  └─ extraction/
│     └─ customer_care_emails.csv
├─ ingestion/
│  └─ customer_care_emails/
│     ├─ MANIFEST.md
│     ├─ .env.sample
│     └─ config/
│        └─ schema_expected.yaml
├─ logs/
└─ plugins/
```

## Prerequisites
- Docker & Docker Compose
- Free ports:
  - Airflow UI → http://localhost:8080
  - MinIO API → http://localhost:9000
  - MinIO Console → http://localhost:9001

## Quickstart

1. **Copy env and set project paths**
   ```bash
   cp .env.sample .env
   # Ensure these are relative to THIS repo:
   # DAGS_VOLUME=./dags
   # INGESTION_VOLUME=./ingestion
   # PLUGINS_VOLUME=./plugins
   ```

2. **Place input CSV**
   Put a non-empty file at:
   ```
   dags/extraction/customer_care_emails.csv
   ```

3. **Bring up the stack**
   ```bash
   docker compose down -v
   docker compose up airflow-init
   docker compose up -d
   ```

4. **Open UIs**
   - Airflow: http://localhost:8080  (user: `admin`, pass: `admin` unless changed)
   - MinIO Console: http://localhost:9001  (user: `minioadmin`, pass: `minioadmin` unless changed)

5. **Run the pipeline**
   - Trigger DAG **`customer_care_emails_to_minio`** in Airflow.
   - Wait until all tasks are green.

6. **Verify output**
   - In MinIO Console, open bucket **`${MINIO_BUCKET}`** (default: `bronze`)
   - Navigate to:
     ```
     bronze/customer_care_emails/ingest_date=YYYY-MM-DD/*.parquet
     ```

## Configuration
Environment (from `.env`):
```dotenv
# Airflow
AIRFLOW_UID=50000
AIRFLOW_GID=0
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth

# Auth for Airflow UI
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin

# MinIO / S3
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_BUCKET=bronze
MINIO_REGION=us-east-1
S3_ENDPOINT=http://minio:9000
S3_SECURE=false
AWS_ACCESS_KEY_ID=${MINIO_ROOT_USER}
AWS_SECRET_ACCESS_KEY=${MINIO_ROOT_PASSWORD}
AWS_DEFAULT_REGION=${MINIO_REGION}

# Mounts (relative to repo root)
DAGS_VOLUME=./dags
INGESTION_VOLUME=./ingestion
PLUGINS_VOLUME=./plugins
```

## What each task does
1. **`file_check`** – Asserts the CSV exists and is non-empty at `/opt/airflow/dags/extraction/customer_care_emails.csv`.
2. **`schema_validation`** – Validates columns (presence & order), types, nullability, and primary key rules against `schema_expected.yaml`.
3. **`transform_to_parquet`** – Trims strings, casts by schema, coerces timestamps, adds `ingest_date`, writes Parquet to:
   ```
   /opt/airflow/ingestion/customer_care_emails/_parquet_out/ingest_date=YYYY-MM-DD/*.parquet
   ```
4. **`upload_to_minio`** – Ensures bucket exists and uploads partition files to `s3://$MINIO_BUCKET/customer_care_emails/ingest_date=YYYY-MM-DD/`.
5. **(Optional) `data_quality`** – Reads Parquet back from MinIO and verifies row count, columns, types, PK.

## Verifying the run
- **MinIO Console** → check the `bronze/customer_care_emails/ingest_date=.../` folder for `.parquet` files.
- **Airflow XComs/logs** →
  - `schema_validation` pushes `row_count`
  - `transform_to_parquet` logs partition path
  - `upload_to_minio` logs file count uploaded

### Manual spot checks (local inside container)
```bash
# Confirm Parquet exists locally (pre-upload)
docker compose exec worker bash -lc   'find /opt/airflow/ingestion/customer_care_emails/_parquet_out -name "*.parquet" -maxdepth 3'

# List objects in MinIO via mc
docker compose exec mc sh -lc   'mc alias set local http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD && mc tree -d 2 local/$MINIO_BUCKET'
```

## Troubleshooting
- **No DAGs showing** → mounts wrong. Inside webserver:
  ```bash
  docker compose exec airflow-webserver ls -la /opt/airflow/dags
  ```
- **CSV not found** → file must exist at `dags/extraction/customer_care_emails.csv` on host.
- **Schema file not found** → ensure `ingestion/customer_care_emails/config/schema_expected.yaml` exists and `INGESTION_VOLUME=./ingestion`.
- **Timestamp type mismatch** → make CSV timestamps parseable or change type to `string` in schema; transform casts to datetime.
- **NoSuchBucket** → create bucket:
  ```bash
  docker compose exec mc sh -lc 'mc alias set local http://minio:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD && mc mb -p local/$MINIO_BUCKET || true && mc ls local'
  ```
- **Nothing in MinIO after success** → re-run only `upload_to_minio` task (Clear → this task).

## Clean up
```bash
docker compose down -v
```

## Success Criteria
- ✅ CSV → validate → transform → Parquet in MinIO
- ✅ Schema contract enforced (DAG fails on mismatch)
- ✅ Parquet visible in MinIO console
- ✅ Reproducible via Docker Compose
- ✅ Code + docs committed (this README, MANIFEST, schema, run steps)

---

**Notes**  
- To ingest a different CSV, replace `dags/extraction/customer_care_emails.csv` with a file that matches the schema (or update the schema accordingly).
- You can optionally parameterize the CSV path via an Airflow Variable if you want to switch inputs without code changes.
