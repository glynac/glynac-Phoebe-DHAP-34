# ALDS CSV to Delta Lake Pipeline

This project contains an end-to-end Apache Airflow pipeline to extract data from raw CSV files, validate its structure, apply cleansing and transformation rules, and publish the final, production-ready datasets to Azure Data Lake Storage (ADLS) Gen2 in the Delta Lake format.

-----

## Pipeline Overview

The pipeline is orchestrated by a chain of three modular DAGs, ensuring data quality at each stage:

1.  **Schema Validation** ([`schema_validation_dag.py`](https://www.google.com/search?q=%5Bhttps://github.com/Glynac-AI/airflow-dags/blob/main/transformation_to_deltalake/schema_validation_dag.py%5D\(https://github.com/Glynac-AI/airflow-dags/blob/main/transformation_to_deltalake/schema_validation_dag.py\))): Acts as the entry point and quality gate. It validates the structure (column names and data types) of incoming raw CSV files against a predefined schema. The pipeline stops if any file fails validation.

2.  **Cleansing & Transformation** ([`cleansing_and_transformation_dag.py`](https://www.google.com/search?q=%5Bhttps://github.com/Glynac-AI/airflow-dags/blob/main/transformation_to_deltalake/cleansing_transformation_dag_to_deltalake.py%5D\(https://github.com/Glynac-AI/airflow-dags/blob/main/transformation_to_deltalake/cleansing_transformation_dag_to_deltalake.py\))): Takes the structurally valid files and applies business logic. This includes handling null values, removing duplicates, and converting data types (e.g., strings to UTC timestamps). The cleaned data is staged as intermediate CSVs.

3.  **Output Validation & Publish** ([`output_validation_and_publish_dag.py`](https://www.google.com/search?q=%5Bhttps://github.com/Glynac-AI/airflow-dags/blob/main/transformation_to_deltalake/output_validation_and_publish_dag_to_deltalake.py%5D\(https://github.com/Glynac-AI/airflow-dags/blob/main/transformation_to_deltalake/output_validation_and_publish_dag_to_deltalake.py\))): Performs final quality checks on the cleaned data (e.g., row count sanity checks, primary key integrity). If all checks pass, it writes the data to its final destination in ADLS as a Delta Lake table.

-----

## Project Structure

```
airflow-dags/
│
├── data/                     # Contains raw source CSV files
│   ├── cleaned/              # Staging for intermediate cleaned CSVs
│   └── final/                # Local simulation of final Delta Lake output
│
├── dags/
│   └── transformation_to_deltalake/
│       ├── schema_validation_dag.py
│       ├── cleansing_and_transformation_dag.py
│       └── output_validation_and_publish_dag.py
│
├── .env                      # For storing credentials (DO NOT COMMIT FOR PRODUCTION)
├── .gitignore
├── docker-compose.yaml
├── Dockerfile                # Customizes the Airflow image with new packages
└── requirements.txt          # Defines Python dependencies (pyarrow, deltalake)
```

-----

## Environment Setup

Before running the pipeline, you must configure the environment variables for connecting to Azure.

1.  **Prerequisites**:

      * Docker & Docker Compose installed.
      * An Azure Subscription with an ADLS Gen2 Storage Account.
      * An Azure Service Principal with the **"Storage Blob Data Contributor"** role on the storage account.

2.  **Create `.env` File**:
    In the root directory of the project, create a file named `.env` and add your Azure Service Principal credentials. The `deltalake` library will automatically use these variables for authentication.

    ```
    # Azure Service Principal Credentials
    AZURE_TENANT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    AZURE_CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    AZURE_CLIENT_SECRET=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    ```

-----

## Running the Pipeline Locally

1.  **Navigate to the Project Root**:
    Open a terminal in the directory containing the `docker-compose.yaml` file.

2.  **Build the Custom Docker Image**:
    This command builds the Airflow image using the `Dockerfile`, which installs `pyarrow` and `deltalake` from your `requirements.txt`. You only need to run this once or when you change `requirements.txt`.

    ```bash
    docker-compose build
    ```

3.  **Start Airflow Services**:
    This starts all Airflow services (webserver, scheduler, worker, etc.) in the background.

    ```bash
    docker-compose up -d
    ```

4.  **Access the Airflow UI**:
    Open your browser and go to **`http://localhost:8080`**.

      * **Username**: `airflow`
      * **Password**: `airflow`

-----

## Executing the Pipeline

The three DAGs are designed to run in a dependent chain. You only need to trigger the first one manually.

1.  **Place Raw Data**: Ensure your source CSV files are in the `./data/` directory.
2.  **Trigger the First DAG**: In the Airflow UI, un-pause and manually trigger the `schema_validation_dag`.
3.  **Automatic Execution**:
      * Upon successful validation, the `cleansing_and_transformation_dag` will be triggered automatically.
      * Upon successful cleansing, the `output_validation_and_publish_dag` will be triggered automatically.

-----

## Expected Output

If the entire pipeline succeeds, the final datasets will be written as Delta Lake tables to your Azure Data Lake Storage account.

  * **Final ADLS Path Example**:

    ```
    abfss://deltalake@glynacdlgen2.dfs.core.windows.net/alds/final/deltalake/{table_name}/
    ```

    *(where `{table_name}` is `lead_log`, `user_logs`, etc.)*

  * **Verification**: You can verify the output by checking the task logs in the Airflow UI and by browsing your ADLS container using the Azure Portal or Azure Storage Explorer.
