# Output Validation & Publish DAG

## Overview
The **`output_validation_and_publish_dag_to_iceberg`** is an Apache Airflow DAG that performs **final output validation** on cleansed CSVs, and if validation passes, publishes them to **Apache Iceberg tables** in **Azure Data Lake Storage Gen2 (ADLS)**.  

This step is the last stage in the pipeline after:
1. **Schema Validation** → Ensure raw CSVs have correct columns & data types.
2. **Cleansing & Transformation** → Generate clean, enriched CSV outputs.
3. **Output Validation & Publish** → Validate final dataset & write to Iceberg.

---

## Prerequisites
- **Docker** & **Docker Compose** installed.
- **Azure Data Lake Storage Gen2** account with a configured container.
- An `.env` file containing Azure credentials and pipeline configuration.
- Cleaned CSVs in `dags/data/staging/clean`.

---

## Project Structure
```
transformation_to_iceberg/
│
├── airflow_docker/
│   ├── dags/
│   │   ├── schema_validation_dag.py
│   │   ├── cleansing_transformation_dag_to_iceberg.py
│   │   ├── output_validation_and_publish_dag_to_iceberg.py
│   │   └── spark_job_publish_iceberg.py
│   ├── logs/
│   ├── plugins/
│   ├── .env
│   └── docker-compose.yaml
├── dags/data/staging/clean/     # final clean CSV files before publishing
├── README.md
```

---

## Environment Variables
Create a `.env` file inside `airflow_docker/` with:
```env
AZURE_STORAGE_ACCOUNT=your_storage_account_name
AZURE_ACCOUNT_KEY=your_storage_account_key
ICEBERG_WAREHOUSE=abfss://<container>@<account>.dfs.core.windows.net/iceberg-warehouse
ICEBERG_CATALOG=lake
PIPELINE_MODE=overwrite
CLEAN_INPUT_DIR=/opt/airflow/data/staging/clean

HOST_DAGS_DIR=C:/path/to/airflow_dags
HOST_CLEAN_DIR=C:/path/to/dags/data/staging/clean

AIRFLOW_UID=50000
```

---

## Running the DAG in Docker
1. **Navigate to project directory**
```bash
cd transformation_to_iceberg/airflow_docker
```

2. **Initialize Airflow**
```bash
docker compose up airflow-init
```

3. **Start Airflow services**
```bash
docker compose up
```

4. **Access Airflow Web UI**  
   Open [http://localhost:8080](http://localhost:8080)  
   Default credentials:
   ```
   Username: airflow
   Password: airflow
   ```

5. **Trigger the pipeline**  
   - First run:  
     `schema_validation_dag` → `cleansing_transformation_dag_to_iceberg` → `output_validation_and_publish_dag_to_iceberg`
   - For re-runs of only the publish step:  
     Open `output_validation_and_publish_dag_to_iceberg`, clear failed `publish_to_iceberg` task, then **Run**.

---

## Expected Output
- **Success** → Spark writes tables to:
  ```
  abfss://<container>@<account>.dfs.core.windows.net/iceberg-warehouse/prod/<table_name>
  ```
  Logs will show:
  ```
  Published <table> to lake.prod.<table>
  ```

- **Failure** → Task marked as **Failed** with details in logs.

---

## Troubleshooting
- **File Not Found**  
  Check that `spark_job_publish_iceberg.py` path in the DAG matches your mounted volume inside Docker.
- **Memory Issues (StatusCode 137)**  
  Increase Docker Desktop WSL2 memory limit via `.wslconfig`:
  ```
  [wsl2]
  memory=8GB
  processors=4
  swap=0
  ```
  Save in `C:\Users\<YourName>\.wslconfig` and run:
  ```powershell
  wsl --shutdown
  ```
  Restart Docker Desktop.
- **Schema mismatches**  
  Ensure CSV headers & types in `staging/clean` match expected.
