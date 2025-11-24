# Subtask 2 — Schema Validation, Cleansing, Enrichment & Load to Microsoft Fabric

This repo contains an Airflow DAG that:
1) **Selects the Top‑3 _widest_ Postgres tables** from the ADLS inventory (Subtask‑1 output).  
2) **Validates schema** against live Postgres (columns + **PK order**).  
3) **Cleanses & standardizes** a sampled (or full) extract from Postgres.  
4) **Enriches** with metadata columns for lineage.  
5) **Writes Parquet** directly to **Microsoft Fabric / OneLake** (ADLS Gen2).  
6) **Emits an audit JSON** with counts and run metadata.

> You can register the landed Parquet as a Lakehouse table (UI → *Create table from files*), or load it into a Fabric **Warehouse** with `COPY INTO` if you need T‑SQL querying. The pipeline itself lands in OneLake and does not require a Warehouse.

---

## Folder layout (project root)
```
pg_etl/
└─ airflow_docker/
   ├─ dags/
   │  └─ pg_top3_widest_validate_cleanse_enrich_load_fabric_dag.py
   ├─ plugins/
   ├─ logs/
   ├─ docker-compose.yaml
   └─ .env
```

---

## Prerequisites
- **Docker Desktop** (Compose v2)  
- Access to **Azure Storage (ADLS Gen2)** with **Storage Blob Data Contributor** on the target account/container  
- A **Microsoft Fabric** workspace and a **Lakehouse** (you may add a OneLake **shortcut** to your ADLS container for convenience)  
- Network access to your **source Postgres**

---

## Environment variables (`.env`)
Create `airflow_docker/.env` with your values:

```ini
# ---- Airflow
AIRFLOW_UID=50000
AIRFLOW_IMAGE_NAME=apache/airflow:2.8.1

# Leave empty to avoid pip-at-startup restarts (install inside containers once they are up).
_PIP_ADDITIONAL_REQUIREMENTS=

# ---- Postgres source
PG_HOST=your.pg.host
PG_PORT=5432
PG_DB=Glynac_database
PG_USER=extract123
PG_PASSWORD=********
PG_SSLMODE=require

# ---- ADLS auth (choose one path)
ADLS_ACCOUNT_NAME=glynacdlgen2
ADLS_ACCOUNT_KEY=**************
# ADLS_SAS_TOKEN=?sv=...        # (alternative to key)

# ---- Subtask 1 inventory (read)
INV_ACCOUNT_URL=https://glynacdlgen2.dfs.core.windows.net
INV_CONTAINER=pginventory
INV_ROOT=pg_inventory

# ---- Subtask 2 landing for Fabric (write)
FABRIC_ACCOUNT_URL=https://glynacdlgen2.dfs.core.windows.net
FABRIC_CONTAINER=pg_etl_clean   # container will be auto-created if missing
FABRIC_ROOT=clean               # base folder in the container

# ---- Optional knobs
SOURCE_SYSTEM_ID=postgresql_pg_fabric_src
SAMPLE_ROWS=200000             # 0 = full extract
CHUNK_ROWS=50000
```

**Notes**
- Container names are normalized to ADLS rules (lowercase, hyphens only).  
- Inventory auto-resolver finds the latest partition with both `columns.csv` and `primary_keys.csv`.  
- Excluded schemas: `pg_*`, `pg_toast*`, `pg_temp*`, `information_schema`.

---

## Bring up Airflow

From `pg_etl/airflow_docker`:
```bash
# Clean start
docker compose down -v
docker compose up -d
docker compose ps
```

Open **Airflow UI**: http://localhost:8081   
Default login: `airflow / airflow` (unless you changed it).

> If the webserver restarts repeatedly, leave `_PIP_ADDITIONAL_REQUIREMENTS=` empty (as above), then install required packages **inside** the running containers (see Troubleshooting).

---

## Required Python deps (for the DAG)
```
pandas==2.2.2
pyarrow==14.0.2
azure-storage-file-datalake==12.14.1
psycopg2-binary==2.9.9
python-tds==1.13.0
```

### Quick install inside running containers
```bash
PKGS="pandas==2.2.2 pyarrow==14.0.2 azure-storage-file-datalake==12.14.1 psycopg2-binary==2.9.9 python-tds==1.13.0"
docker compose exec airflow-webserver  bash -lc "pip install --no-cache-dir $PKGS"
docker compose exec airflow-scheduler bash -lc "pip install --no-cache-dir $PKGS"
docker compose exec airflow-worker    bash -lc "pip install --no-cache-dir $PKGS"
docker compose exec airflow-triggerer bash -lc "pip install --no-cache-dir $PKGS"
docker compose restart airflow-webserver airflow-scheduler airflow-worker airflow-triggerer
```

> **Recommended long-term**: bake these into an image with a small `Dockerfile` and point Compose at it. This avoids pip‑at‑startup completely.

---

## Running the DAG
1. Airflow UI → **DAGs** → `pg_top3_widest_validate_cleanse_enrich_load_fabric_dag` → **Unpause**.  
2. Click **Trigger DAG**.

### What the tasks do
- **`pick_top3`** — reads `columns.csv` and returns the Top‑3 widest tables.  
- **`validate_schema`** — compares *inventory vs live Postgres* (columns set AND primary key order). Fails fast on mismatch.  
- **`extract_clean_enrich_and_write`** — streams Postgres → cleanses → enriches → writes Parquet parts to ADLS/OneLake. Also drops PK duplicates if PK exists.  
- (Optional) **`ensure_fabric_table_and_load`** — if present/enabled, this creates/loads a Fabric table (Warehouse) via `COPY INTO`. If your version doesn’t include this task, you can register the landed files as a Lakehouse table in the UI.

### Enrichment columns added
- `__ingestion_time` (UTC), `__extraction_time` (UTC)  
- `__source_system` (from `SOURCE_SYSTEM_ID`)  
- `__run_id` (Airflow run id)  
- `__table_fqn` (schema.table)

---

## Where to find the output in Fabric / ADLS
Landed Parquet is partitioned like this:
```
abfss://<FABRIC_CONTAINER>@<ADLS_ACCOUNT>.dfs.core.windows.net/
  clean/
    schema=<schema>/
      table=<table>/
        date=<YYYY-MM-DD>/
          part-00000-<runid>.parquet
```

**Audit / metrics** (one JSON per table/run):
```
clean/_audit/date=<YYYY-MM-DD>/schema=<schema>/table=<table>/load_metrics.json
```

### Viewing in Fabric Lakehouse
- Lakehouse Explorer → **Tables → Unidentified → <your OneLake shortcut> → clean/**…** → date=<YYYY-MM-DD>**.  
- Click a `.parquet` file to **Preview** rows.  
- To query with T‑SQL: **Create table from files** (Lakehouse) or use a **Warehouse** and `COPY INTO` from the same path.

---

## Verifying row counts

### A) Spark Notebook in Lakehouse
```python
path = "abfss://pg-etl-clean@glynacdlgen2.dfs.core.windows.net/clean/schema=<schema>/table=<table>/date=<YYYY-MM-DD>"
df = spark.read.parquet(path)
df.count(), display(df.limit(5))
```

### B) After registering a table (Lakehouse UI)
```
SELECT COUNT(*) FROM dbo.<your_table_name>;
```

### C) Warehouse (T‑SQL `COPY INTO` flow)
```sql
CREATE TABLE dbo.<your_table_name> ( ... );   -- define once
COPY INTO dbo.<your_table_name>
FROM 'abfss://pg-etl-clean@glynacdlgen2.dfs.core.windows.net/clean/schema=<schema>/table=<table>/date=<YYYY-MM-DD>/'
WITH ( FILE_TYPE = 'PARQUET' );

SELECT COUNT(*) FROM dbo.<your_table_name>;
```

---

## Troubleshooting

**Webserver keeps restarting / UI not reachable**
- Leave `_PIP_ADDITIONAL_REQUIREMENTS=` empty in `.env`.  
- Start the stack: `docker compose up -d`.  
- Install required packages **inside** containers (see section above).  
- Check logs: `docker compose logs -f airflow-webserver`.  
- Change host port in compose if 8081 is busy: `ports: ["8090:8080"]` → browse http://localhost:8090

**403 or path issues writing to ADLS**
- Ensure your user/service has **Storage Blob Data Contributor** on the account/container.  
- Double‑check `FABRIC_ACCOUNT_URL`, `FABRIC_CONTAINER`, `FABRIC_ROOT` in `.env`.  
- Container names are normalized (underscores → hyphens, lowercase, 3–63 chars).

**Postgres connection errors**
- Verify `PG_*` variables, SSL mode, firewall, and network visibility from the Docker host.

**Fabric shows “Unidentified”**
- Expected for raw Parquet folders. Convert to a Lakehouse table (UI → *Create table from files*) or load into a Warehouse with `COPY INTO`.

**Schema / PK mismatch**
- The DAG fails fast with a clear message listing inventory vs live differences. Update your inventory or source schema as appropriate.

---

## What this delivery covers (Subtask‑2 checklist)
- ✅ Pick Top‑3 **widest** tables from inventory
- ✅ Live **schema & PK** validation
- ✅ **Cleansing & standardization** (trim, nulls, booleans, dedupe by PK)
- ✅ **Enrichment** columns for lineage
- ✅ **Direct load** to Fabric/OneLake (Parquet)
- ✅ **Audit metrics** emitted per table

Optionally:
- 🟦 Register Lakehouse table (UI) or **Warehouse** `COPY INTO` for direct SQL querying

---

## Useful commands (Windows PowerShell friendly)

```powershell
# Stop & remove everything (including volumes)
docker compose down -v

# Start
docker compose up -d

# Watch logs
docker compose logs -f airflow-webserver

# Health probe
docker compose exec airflow-webserver bash -lc "curl -sf http://127.0.0.1:8080/health || echo DOWN"

# Reinstall deps (if needed)
$pkgs = "pandas==2.2.2 pyarrow==14.0.2 azure-storage-file-datalake==12.14.1 psycopg2-binary==2.9.9 python-tds==1.13.0"
docker compose exec airflow-webserver  bash -lc "pip install --no-cache-dir $pkgs"
docker compose exec airflow-scheduler bash -lc "pip install --no-cache-dir $pkgs"
docker compose exec airflow-worker    bash -lc "pip install --no-cache-dir $pkgs"
docker compose exec airflow-triggerer bash -lc "pip install --no-cache-dir $pkgs"
```

---

## Credits / Notes
- Airflow 2.8.x, Celery executor on Postgres/Redis in Docker Compose.  
- Writes are performed using `azure-storage-file-datalake` (ADLS Gen2) and `pyarrow` Parquet.  
- Inventory read expects `columns.csv` & `primary_keys.csv` from Subtask‑1.
