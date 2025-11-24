# Airflow Docker Environment (CeleryExecutor)

Brings up Apache Airflow (webserver, scheduler, worker, triggerer), Redis (broker), Postgres (metadata), and an *optional* dev Postgres target for local testing.

## Prerequisites
- Docker Desktop (or Docker Engine + Docker Compose)
- Ports available: 8080 (Airflow UI), 5433 (metadata DB optional), 5434 (dev target PG)

## First-time setup
```bash
cd airflow-dags/tasnim-project1
# copy template → local env (do NOT commit .env)
cp .env.example .env

# initialize Airflow DB and create admin user
docker compose up airflow-init

# start the stack
docker compose up -d
```

## Usage
- Airflow UI: http://localhost:8080  (default admin/admin from .env)
- Dev target Postgres (optional): host `localhost`, port `5434`, DB/user/pass from `.env`
- Airflow logs: `airflow-dags/yourname-project1/logs/`

Stop:
```bash
docker compose down
```

## Notes
- The entire repo root (`airflow-dags/`) is mounted at `/opt/airflow/dags` inside the containers.
  - Your dataset scaffold lives under `extraction/customer_care_emails/` and will be visible to Airflow.
- To add Python libs, either edit `requirements.txt` or set `_PIP_ADDITIONAL_REQUIREMENTS` in `.env`.
- For production, set a proper executor config, secrets manager, and external Postgres; keep secrets out of Git.
