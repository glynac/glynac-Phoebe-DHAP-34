# Email Thread ETL Pipeline

## Steps

1. Start the environment:
   docker compose up -d

2. Place CSV files inside:
   ./data/

3. Airflow Web UI:
   http://localhost:8080

4. Trigger DAG:
   `load_email_thread_data`

5. Tables created:
   - email_thread_details
   - email_thread_summaries

6. PostgreSQL Connection:
   postgresql://airflow:airflow@localhost:5432/airflow
