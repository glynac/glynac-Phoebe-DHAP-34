# Email Thread Summary – Airflow Ingestion Pipeline

## Overview

This project implements a **containerized Apache Airflow pipeline** that ingests a local CSV dataset (downloaded from SharePoint), validates it against a schema contract, applies basic transformations, and loads clean records into a PostgreSQL table.

The pipeline is designed to be **reproducible, schema-driven, and production-ready**.

---

## Dataset Details

- **Dataset name**: `email_thread_summary`
- **Source**: Local CSV download (from Kaggle)

## Project Structure

extraction/email_thread_summary/
├── MANIFEST.md
├── README.md
├── .env.sample
├── config/
│   ├── schema_expected.yaml
│   └── create_table.sql
├── sample_data/
│   └── email_thread_summary.csv
├── dags/
│   └── email_thread_summary_ingest.py
└── logs/
    └── clean_data.csv

## Environment Variables

LOCAL_PG_HOST=postgres
LOCAL_PG_PORT=5432
LOCAL_PG_DB=airflow
LOCAL_PG_USER=airflow
LOCAL_PG_PASSWORD=airflow

## How to Run the Pipeline

- Start Docker Compose: docker compose -f airflow-dags/satwick-project1/docker-compose.yml up -d
- Open Airflow UI: http://localhost:8080
- Trigger the DAG:
    Locate DAG: email_thread_summary_ingest
    Click start Trigger DAG
- Verify Data in PostgreSQL: docker exec -it local_postgres psql -U airflow -d airflow

## DAG Overview

- DAG Name: email_thread_summary_ingest
- Task Flow:
    check_file_exists
            ↓
    validate_schema
            ↓
    transform_data
            ↓
    load_to_postgres

## Project Structure Explanation

MANIFEST.md
Contains dataset metadata including dataset name, local CSV path, and target PostgreSQL table.

schema_expected.yaml
Defines the expected schema of the dataset including column names, data types, nullability, and primary key (if applicable).

create_table.sql
PostgreSQL DDL used to create the target table if it does not already exist.

sample_data/
Contains the input CSV file used by the ingestion pipeline.

dags/
Contains the Airflow DAG that orchestrates validation, transformation, and loading of data.

logs/
Stores intermediate artifacts generated during DAG execution (e.g., cleaned CSV output).

## Troubleshooting

CSV File Not Found

Error: CSV file not found during check_file_exists
Fix: Ensure the CSV exists in sample_data/ and the filename matches the path in MANIFEST.md.

Schema Mismatch

Error: Schema mismatch between CSV and schema_expected.yaml
Fix: Update the YAML schema or correct the CSV column names/order.

Invalid Database Credentials

Error: load_to_postgres fails due to connection error
Fix: Verify values in .env, then restart Docker containers.

DAG Not Visible in Airflow UI

Fix:

Ensure DAG file exists under dags/

Restart Airflow webserver and scheduler

Check for Python syntax errors in the DAG file

Resetting DAG Runs

Fix: Delete failed DAG runs from the Airflow UI and re-trigger the DAG.

## RUNBOOK

- Updating Schema:
    Modify config/schema_expected.yaml
    Update config/create_table.sql if needed
    Re-run the DAG

- Loading a New CSV:
    Replace the CSV in sample_data/
    Trigger the DAG again from Airflow UI

## Pre-Commit Checklist

Before committing changes or a new dataset:
    MANIFEST.md updated
    schema_expected.yaml updated
    create_table.sql updated
    Sample CSV added to sample_data/
    DAG tested successfully
    README.md updated

