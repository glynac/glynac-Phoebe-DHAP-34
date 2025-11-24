# Customer Care Emails Ingestion Pipeline

## 📌 Purpose
This pipeline ingests customer care email records from CSV into a PostgreSQL table.  
It validates schema, applies transformations (strip whitespace, null handling, skip "done" rows),  
and loads clean data into the target database.

- **Source CSVs:** `sample_data/customer_care_emails.csv`
- **Target Table:** `public.customer_care_emails` in the `analytics` database (`pg_target` container)

---

## ⚙️ Environment Setup

### Required Variables
Defined in `.env` (copy from `.env.sample` at project root):

```env
# Airflow user setup
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@example.com

# Airflow metadata DB
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=airflow
AIRFLOW_DB_NAME=airflow

# Target Postgres (for dataset loads)
TARGET_PG_HOST=pg_target
TARGET_PG_PORT=5432
TARGET_PG_DB=analytics
TARGET_PG_USER=etl_user
TARGET_PG_PASSWORD=supersecret
```

---

## 🚀 How to Run

1. **Start Airflow & dependencies**
   ```bash
   cd airflow-dags/tasnim-project1
   docker compose up -d
   ```

   - Webserver → [http://localhost:8081](http://localhost:8081)  
   - Target DB (pg_target) → exposed on `localhost:5434`

2. **Access Airflow UI**
   - Log in with `admin / admin` (or whatever you set in `.env`)
   - Enable the DAG `customer_care_emails_ingest`
   - Trigger a run manually

3. **Verify data landed**
   ```bash
   docker compose exec pg_target psql -U etl_user -d analytics
   ```

   Inside psql:
   ```sql
   \d public.customer_care_emails;
   SELECT COUNT(*) FROM public.customer_care_emails;
   SELECT * FROM public.customer_care_emails LIMIT 5;
   \q
   ```

---

## 📊 DAG Overview

Tasks & dependencies:

```
file_check → validate_and_transform → create_table_if_not_exists → load_to_postgres
```

- **file_check** → Ensures CSV exists in `sample_data/`
- **validate_and_transform** → Validates schema (`schema_expected.yaml`), cleans data
- **create_table_if_not_exists** → Runs `create_table.sql` to ensure PG table exists
- **load_to_postgres** → Bulk inserts cleaned CSV into target DB

---

## 🛠️ Troubleshooting

- **Missing CSV**
  ```
  FileNotFoundError: CSV file not found
  ```
  → Ensure `sample_data/customer_care_emails.csv` exists.

- **Schema mismatch**
  ```
  ValueError: Missing column X
  ```
  → Update `config/schema_expected.yaml` or fix your CSV.

- **Invalid credentials**
  ```
  FATAL: password authentication failed
  ```
  → Check `.env` Postgres user/pass matches.

- **DAG stuck or failing repeatedly**
  - Clear DAG runs in Airflow UI (select DAG → "Clear" button)
  - Restart containers:
    ```bash
    docker compose down
    docker compose up -d
    ```

---

## 📒 Runbook

### Updating schema / DDL
- Edit `config/schema_expected.yaml` for column types or nullability
- Update `config/create_table.sql` for new PG table structure
- Restart DAG → will re-run validation + load

### New CSV drops
- Place new CSV into `sample_data/`
- Trigger DAG again → it will re-validate and load new rows (skipping "done")

### Checklist before committing a new dataset
- [ ] Added entry in `MANIFEST.md`
- [ ] `config/schema_expected.yaml` updated
- [ ] `config/create_table.sql` updated
- [ ] `sample_data/` contains at least one CSV
- [ ] DAG file exists under `dags/`

---

## ✅ Reproducibility Guarantee
Following this README, a new engineer can:
1. Spin up Airflow & Postgres with Docker
2. Validate dataset with schema contract
3. Load data into analytics DB
4. Debug common issues
5. Extend pipeline with new datasets
