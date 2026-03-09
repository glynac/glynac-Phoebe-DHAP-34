# email_thread_summaries (DHAP-34)

This folder has everything needed to ingest the `email_thread_summaries` CSV into Postgres using Airflow running locally with Docker.

## What this is

The dataset is a CSV where each row represents an email thread and its summary. The pipeline:

1. checks the input files exist
2. validates the CSV matches the schema contract
3. does light cleanup
4. creates the target table (if needed) and loads the data into Postgres

## Where things live (repo paths)

`intern-project/Krrish_Kohli/project-DHAP-34/airflow-dags/extraction/email_thread_summaries/`

- `sample_data/email_thread_summaries.csv`  
  Input CSV used by the pipeline.

- `config/schema_expected.yaml`  
  Schema contract (columns, types, nullability).

- `config/create_table.sql`  
  SQL DDL used to create the Postgres table.

- `dags/email_thread_summaries_ingest.py`  
  The Airflow DAG that runs the ingestion.

- `MANIFEST.md`  
  Small dataset manifest (dataset name, local CSV path, target table).

- `.env.sample`  
  Placeholder env vars (no secrets).

## Target Postgres table

Loads into:
`public.email_thread_summaries`

Verified load:
`SELECT COUNT(*) FROM public.email_thread_summaries;` returned **4167** rows.

## Required environment variables

The DAG reads Postgres connection settings from environment variables (provided by Docker Compose):

- `PG_HOST`
- `PG_PORT`
- `PG_DB`
- `PG_USER`
- `PG_PASSWORD`

`.env.sample` in this dataset folder is just a reference. The actual values are set in the Docker Airflow environment.

## How to run (Dockerized Airflow)

The local Airflow + Postgres environment is under:

`intern-project/Krrish_Kohli/project-DHAP-34/airflow-dags/krrishkohli-project1/`

From that directory:

```bash
cp .env.example .env
docker compose up airflow-init
docker compose up -d
```

## Airflow UI:

- http://localhost:8080
- Login credentials are in .env

## Target Postgres:

- available on localhost:5433

# Run the DAG

In the Airflow UI:

- Find DAG: email_thread_summaries_ingest
- Unpause if needed
- Click “Trigger DAG”

# DAG overview (task flow)

The DAG tasks run in this order:

1. `file_check`
   Confirms the CSV + schema + DDL files exist.
2. `validate_schema`
   Ensures the CSV header matches `schema_expected.yaml` (column names + order).
   Also checks nullability and basic integer validation.
3. `transform`
   Strips whitespace and does basic null cleanup.
   If a `status` or `dataset_status` column exists, rows marked `done` are skipped.
4. `load`
   Runs `create_table.sql` then loads the cleaned CSV into Postgres.
5. `verify`
   Confirms data exists by running a row count query.

## Verify the load (Postgres)

From the `krrishkohli-project1` directory, run:

```bash
docker compose exec pg-target psql -U <PG_USER> -d <PG_DB> -c "select count(*) from public.email_thread_summaries;"
```

Replace `<PG_USER>` and `<PG_DB>` with values from your `.env`.

# Troubleshooting

## Schema mismatch

If `validate_schema` fails, compare:

- CSV header row
- config/schema_expected.yaml
  This pipeline is strict about column order.

## Missing CSV / config files

If `file_check` fails, confirm these exist:

- `sample_data/email_thread_summaries.csv`
- `config/schema_expected.yaml`
- `config/create_table.sql`

## Postgres credentials / connection errors

If `load` fails connecting to Postgres:

- check `PG_HOST/PG_PORT/PG_DB/PG_USER/PG_PASSWORD` in `.env`
- confirm containers are up: `docker compose ps`

## Ports already in use

If Docker fails to start services due to ports:

- Airflow UI uses `8080`
- Target Postgres uses `5433`
  Stop whatever is using the port or change the port mapping in `docker-compose.yml`.

## Reset / rerun after failure

In Airflow Graph view:

- click the failed task
- choose “Clear” (include downstream tasks if needed)
- trigger again

## Reset target DB (dev only)

If you remove the pg-target volume, the DB will be empty and the table won’t exist until you rerun the DAG.

```bash
docker compose down
docker volume rm krrishkohli-project1_target_db
docker compose up -d
```
