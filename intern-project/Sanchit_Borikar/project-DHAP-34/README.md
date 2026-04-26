
# Email Thread Summary — Airflow Ingestion Pipeline

## 1. Overview

This pipeline ingests the **Email Thread Summary** dataset into PostgreSQL using Apache Airflow. It validates the CSV against a schema contract, applies lightweight transforms, and loads the cleaned data into a target table.

| Field             | Value                                          |
|-------------------|------------------------------------------------|
| **Dataset Name**  | `email_thread_summary`                         |
| **Source**        | Kaggle — Email Thread Summary Dataset          |
| **CSV Location**  | `extraction/email_thread_summary/sample_data/final_dataset.csv` |
| **Target Table**  | `public.email_thread_summary`                  |
| **DAG ID**        | `email_thread_summary_ingest`                  |
| **Owner**         | `sanchit`                                      |

---

## 2. Project Structure

```
extraction/email_thread_summary/
├── .env.sample                          # Environment variable template
├── MANIFEST.md                          # Dataset metadata and lineage
├── README.md                            # This file
├── config/
│   ├── schema_expected.yaml             # Column contract (name, type, nullable)
│   └── create_table.sql                 # DDL for target PostgreSQL table
├── dags/
│   └── email_thread_summary_ingest.py   # Airflow DAG (source of truth)
└── sample_data/
    ├── email_thread_details.csv         # Raw source file
    ├── email_thread_summaries.csv       # Raw source file
    └── final_dataset.csv               # Merged + cleaned CSV used by the DAG

airflow-dags/sanchit-project1/           # Airflow runtime environment
├── .env.example                         # Runtime env vars (PG_HOST=postgres)
├── docker-compose.yml                   # Docker services definition
├── requirements.txt                     # Python dependencies
├── dags/                                # DAG + supporting files deployed here
│   ├── email_thread_summary_ingest.py
│   ├── final_dataset.csv
│   ├── schema_expected.yaml
│   └── create_table.sql
└── logs/                                # Airflow task logs (auto-generated)
```

---

## 3. Schema Contract

Defined in `config/schema_expected.yaml`:

| Column       | Type        | Nullable |
|--------------|-------------|----------|
| `id`         | `integer`   | No       |
| `thread_id`  | `integer`   | No       |
| `subject`    | `text`      | No       |
| `timestamp`  | `timestamp` | Yes      |
| `from_email` | `text`      | No       |
| `to_email`   | `text`      | No       |
| `body`       | `text`      | No       |
| `summary`    | `text`      | Yes      |

Primary Key: `id`

---

## 4. Environment Variables

Reference file: `.env.sample`

| Variable       | Description                           | Default (Docker)  |
|----------------|---------------------------------------|-------------------|
| `PG_HOST`      | PostgreSQL hostname                   | `postgres`        |
| `PG_PORT`      | PostgreSQL port                       | `5432`            |
| `PG_DB`        | Target database name                  | `airflow`         |
| `PG_USER`      | Database user                         | `airflow`         |
| `PG_PASSWORD`  | Database password                     | `airflow`         |

> **Note:** Inside Docker, `PG_HOST` must be `postgres` (the Docker service name). Outside Docker (local dev), use `localhost` with port `5433`.

---

## 5. DAG Overview

**DAG ID:** `email_thread_summary_ingest`  
**Schedule:** Manual trigger only (`schedule_interval=None`)  
**Executor:** `SequentialExecutor`  
**Catchup:** Disabled

### Task Flow

```
file_check → validate_schema → transform → load_to_postgres
```

| Task               | Description                                                       |
|--------------------|-------------------------------------------------------------------|
| `file_check`       | Verifies `final_dataset.csv` exists at `/opt/airflow/dags/`      |
| `validate_schema`  | Reads `schema_expected.yaml` and checks CSV columns match exactly |
| `transform`        | Strips whitespace from strings, filters out rows with `status=done` |
| `load_to_postgres` | Runs DDL from `create_table.sql`, then inserts data via `psycopg2` |

---

## 6. How to Run the Pipeline

### Prerequisites

- Docker Desktop installed and running
- Docker Compose available (`docker compose version`)

### Step-by-Step

```bash
# 1. Navigate to the Airflow runtime directory
cd airflow-dags/sanchit-project1

# 2. Copy DAG + supporting files from source of truth
Copy-Item "D:\DHAP34\extraction\email_thread_summary\dags\email_thread_summary_ingest.py" ".\dags\"
Copy-Item "D:\DHAP34\extraction\email_thread_summary\sample_data\final_dataset.csv" ".\dags\"
Copy-Item "D:\DHAP34\extraction\email_thread_summary\config\schema_expected.yaml" ".\dags\"
Copy-Item "D:\DHAP34\extraction\email_thread_summary\config\create_table.sql" ".\dags\"

# 3. Initialize the Airflow database and create admin user
docker compose up airflow-init

# 4. Start all services in detached mode
docker compose up -d airflow-webserver airflow-scheduler

# 5. Verify containers are running
docker ps

# 6. Open Airflow UI
#    URL:      http://localhost:8080
#    Username: airflow
#    Password: airflow

# 7. In the UI:
#    a. Find "email_thread_summary_ingest" DAG
#    b. Toggle the pause switch to "ON" (unpause)
#    c. Click "Trigger DAG" (play button)
```

### Verify DAG is Loaded (CLI)

```bash
docker exec -t sanchit-project1-airflow-scheduler-1 airflow dags list
```

### Trigger DAG via CLI

```bash
docker exec -t sanchit-project1-airflow-scheduler-1 airflow dags trigger email_thread_summary_ingest
```

### Stop Environment

```bash
docker compose down        # Stop containers
docker compose down -v     # Stop containers + delete volumes (full reset)
```

---

## 7. Troubleshooting

### 7.1 Schema Mismatch Error

**Error:** `ValueError: Column mismatch. Expected: [...] Got: [...]`

**Cause:** The CSV columns don't match `schema_expected.yaml`.

**Fix:**
1. Open the CSV and check the header row.
2. Update `config/schema_expected.yaml` to match the new columns.
3. Update `config/create_table.sql` DDL accordingly.
4. Re-copy both files to `airflow-dags/sanchit-project1/dags/`.
5. Re-trigger the DAG.

---

### 7.2 Missing CSV File

**Error:** `FileNotFoundError: CSV file not found at /opt/airflow/dags/final_dataset.csv`

**Cause:** The CSV was not copied into the Airflow `dags/` folder.

**Fix:**
```bash
Copy-Item "D:\DHAP34\extraction\email_thread_summary\sample_data\final_dataset.csv" `
  "D:\DHAP34\intern-project\Sanchit_Borikar\project-DHAP-34\airflow-dags\sanchit-project1\dags\"
```

---

### 7.3 Database Connection Refused

**Error:** `psycopg2.OperationalError: could not connect to server`

**Cause:** PostgreSQL container is not running, or `PG_HOST` is wrong.

**Fix:**
1. Check containers: `docker ps`
2. Ensure `.env.example` has `PG_HOST=postgres` (not `localhost` or `host.docker.internal`).
3. Restart: `docker compose down && docker compose up -d`

---

### 7.4 IndentationError in DAG

**Error:** `IndentationError: expected an indented block`

**Cause:** Python file has mixed tabs/spaces or incorrect indentation.

**Fix:**
1. Open `dags/email_thread_summary_ingest.py` in VS Code.
2. Press `Ctrl+Shift+P` → "Convert Indentation to Spaces".
3. Ensure all function bodies are indented with 4 spaces.
4. Re-copy to Airflow dags folder.

---

### 7.5 DAG Not Showing in UI

**Cause:** File not in `dags/` folder, or has a syntax error.

**Fix:**
```bash
# Check for import errors
docker exec -t sanchit-project1-airflow-scheduler-1 airflow dags list-import-errors
```

---

### 7.6 How to Reset DAG Runs / Reload Data

```bash
# Clear all past runs for the DAG
docker exec -t sanchit-project1-airflow-scheduler-1 \
  airflow dags delete email_thread_summary_ingest

# Truncate the target table in Postgres
docker exec -t sanchit-project1-postgres-1 \
  psql -U airflow -d airflow -c "TRUNCATE TABLE public.email_thread_summary;"

# Re-trigger
docker exec -t sanchit-project1-airflow-scheduler-1 \
  airflow dags trigger email_thread_summary_ingest
```

---

## 8. Runbook

### 8.1 Updating Schema When Dataset Evolves

When columns are added, renamed, or removed:

1. **Update the YAML schema contract:**
   ```
   extraction/email_thread_summary/config/schema_expected.yaml
   ```
   Add/remove column entries to match the new CSV headers.

2. **Update the DDL:**
   ```
   extraction/email_thread_summary/config/create_table.sql
   ```
   Modify the `CREATE TABLE` statement to reflect new columns.

3. **Drop the old table (if schema changed):**
   ```bash
   docker exec -t sanchit-project1-postgres-1 \
     psql -U airflow -d airflow -c "DROP TABLE IF EXISTS public.email_thread_summary;"
   ```

4. **Re-copy config files to Airflow:**
   ```bash
   Copy-Item "...\config\schema_expected.yaml" "...\airflow-dags\sanchit-project1\dags\"
   Copy-Item "...\config\create_table.sql" "...\airflow-dags\sanchit-project1\dags\"
   ```

5. **Re-trigger the DAG.**

---

### 8.2 Rerunning with a New CSV Drop

1. Place the new CSV at:
   ```
   extraction/email_thread_summary/sample_data/final_dataset.csv
   ```

2. Copy it to Airflow:
   ```bash
   Copy-Item "...\sample_data\final_dataset.csv" "...\airflow-dags\sanchit-project1\dags\"
   ```

3. (Optional) Truncate old data:
   ```bash
   docker exec -t sanchit-project1-postgres-1 \
     psql -U airflow -d airflow -c "TRUNCATE TABLE public.email_thread_summary;"
   ```

4. Trigger the DAG:
   ```bash
   docker exec -t sanchit-project1-airflow-scheduler-1 \
     airflow dags trigger email_thread_summary_ingest
   ```

---

### 8.3 Checklist Before Committing a New Dataset

Before adding a new dataset to the repository, ensure all of the following exist:

- [ ] **`MANIFEST.md`** — Dataset name, source, description, target table
- [ ] **`config/schema_expected.yaml`** — All columns listed with name, type, nullable
- [ ] **`config/create_table.sql`** — Valid DDL matching the YAML schema
- [ ] **`sample_data/final_dataset.csv`** — At least a sample CSV with correct headers
- [ ] **`dags/<dataset>_ingest.py`** — Working DAG with all 4 tasks
- [ ] **`.env.sample`** — All required environment variables documented
- [ ] **No hardcoded Windows paths** in the DAG (use `/opt/airflow/dags/` only)
- [ ] **No indentation errors** — Run `python -c "import py_compile; py_compile.compile('dags/<file>.py', doraise=True)"` to verify
- [ ] **DAG tested locally** — `docker exec ... airflow dags list` shows DAG without import errors

---

## 9. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│  Source of Truth (Git)                                      │
│                                                             │
│  extraction/email_thread_summary/                           │
│  ├── config/schema_expected.yaml                            │
│  ├── config/create_table.sql                                │
│  ├── dags/email_thread_summary_ingest.py                    │
│  └── sample_data/final_dataset.csv                          │
│                              │                              │
│                         Copy-Item                           │
│                              │                              │
│                              ▼                              │
│  airflow-dags/sanchit-project1/dags/   (Airflow Runtime)    │
│  ├── email_thread_summary_ingest.py                         │
│  ├── final_dataset.csv                                      │
│  ├── schema_expected.yaml                                   │
│  └── create_table.sql                                       │
└─────────────────────────────────────────────────────────────┘
                               │
                    Docker Compose Network
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
       ┌──────────┐    ┌────────────┐    ┌───────────┐
       │ Postgres  │    │ Scheduler  │    │ Webserver │
       │ :5433     │◄───│            │    │ :8080     │
       └──────────┘    └────────────┘    └───────────┘
```

---

## 10. Quick Reference Commands

| Action                        | Command                                                                              |
|-------------------------------|--------------------------------------------------------------------------------------|
| Start environment             | `docker compose up -d airflow-webserver airflow-scheduler`                           |
| Stop environment              | `docker compose down`                                                                |
| Full reset (wipe DB)          | `docker compose down -v`                                                             |
| View running containers       | `docker ps`                                                                          |
| List DAGs                     | `docker exec -t sanchit-project1-airflow-scheduler-1 airflow dags list`              |
| Check import errors           | `docker exec -t sanchit-project1-airflow-scheduler-1 airflow dags list-import-errors`|
| Trigger DAG                   | `docker exec -t sanchit-project1-airflow-scheduler-1 airflow dags trigger email_thread_summary_ingest` |
| View task logs                | Airflow UI → DAG → Task → Logs                                                      |
| Connect to Postgres           | `docker exec -it sanchit-project1-postgres-1 psql -U airflow -d airflow`             |
| Query ingested data           | `SELECT COUNT(*) FROM public.email_thread_summary;`                                  |
