# Airflow Docker Environment (DHAP-34 Story 2)

## Prerequisites

- Docker Desktop
- Docker Compose (docker compose)

## Setup

From this folder:

1. Copy env template
   cp .env.example .env

2. Start Airflow (first time initializes DB + creates admin user)
   docker compose up airflow-init

3. Start services
   docker compose up -d

Airflow UI:

- http://localhost:8080
- Login: airflow / airflow (or whatever you set in .env)

## Stop

docker compose down

## Reset everything (removes volumes)

docker compose down -v

## Local target Postgres (dev/testing)

- Host: localhost
- Port: 5433
- DB: value of PG_DB in .env
- User: value of PG_USER in .env
- Password: value of PG_PASSWORD in .env
