# Chatbot Conversations — Pipeline Documentation & Runbook

## Dataset Overview
- **Dataset:** 3K Conversations Dataset for ChatBot
- **Source:** Kaggle
- **CSV Location:** `extraction/chatbot-conversations/sample_data/3K Conversations Dataset for ChatBot.csv`
- **Target PostgreSQL Table:** `public.chatbot_conversations`
- **Description:** 3,000 conversational question-answer pairs for chatbot training.

## Schema
| Column   | Type    | Nullable | Description              |
|----------|---------|----------|--------------------------|
| id       | INTEGER | FALSE    | Unique row identifier    |
| question | TEXT    | FALSE    | The question in the conversation |
| answer   | TEXT    | FALSE    | The corresponding answer |

## Required Environment Variables
Copy `.env.sample` and fill in your values:

| Variable        | Description                        |
|-----------------|------------------------------------|
| EXT_PG_HOST     | PostgreSQL host                    |
| EXT_PG_PORT     | PostgreSQL port (default 5432)     |
| EXT_PG_DB       | Target database name               |
| EXT_PG_USER     | Database user                      |
| EXT_PG_PASSWORD | Database password                  |
| EXT_PG_SSLMODE  | SSL mode (default prefer)          |

## How to Run the Pipeline

### 1. Start Docker environment
```bash
# From repo root
docker compose up -d
```

### 2. Access Airflow UI
Open http://localhost:8081
- Username: `airflow`
- Password: `airflow`

### 3. Trigger the DAG
- Go to DAGs → `chatbot_conversations_ingest`
- Toggle ON → click ▶ to trigger manually

### 4. Monitor tasks
Click the DAG → Graph view to watch each task run:
file_check → validate_schema → transform → load_to_postgres

### 5. Verify data loaded
Connect to PostgreSQL on port 5433:
```sql
SELECT COUNT(*) FROM public.chatbot_conversations;
SELECT * FROM public.chatbot_conversations LIMIT 5;
```

## DAG Overview

### DAG ID
`chatbot_conversations_ingest`

### Schedule
`@daily` — runs once per day

### Task Flow
file_check → validate_schema → transform → load_to_postgres

### Task Details
| Task | Description |
|------|-------------|
| file_check | Verifies CSV exists in sample_data/ — fails DAG if missing |
| validate_schema | Compares CSV columns against schema_expected.yaml — fails if mismatch |
| transform | Strips whitespace, drops null rows, saves cleaned CSV |
| load_to_postgres | Inserts new rows into PostgreSQL — skips already existing IDs |

## Troubleshooting

### CSV file not found
FileNotFoundError: CSV file not found at: /opt/airflow/extraction/...

**Fix:** Make sure your CSV is in `extraction/chatbot-conversations/sample_data/`

### Schema mismatch
ValueError: Schema mismatch — missing columns: {'id'}

**Fix:** Check your CSV has `id`, `question`, `answer` columns. The unnamed index column is auto-renamed to `id`.

### Invalid PostgreSQL credentials
OperationalError: could not connect to server

**Fix:** Check your `.env` file has correct `EXT_PG_*` values and Docker is running.

### DAG not showing in Airflow UI
**Fix:** Wait 30 seconds for the scheduler to pick up the DAG. Check for syntax errors in the DAG file.

## Runbook

### How to update schema when dataset evolves
1. Update `config/schema_expected.yaml` with new columns
2. Update `config/create_table.sql` with ALTER TABLE or new DDL
3. Update the DAG transform task if new cleaning logic is needed
4. Push changes and redeploy

### How to rerun with a new CSV drop
1. Replace the CSV in `sample_data/`
2. In Airflow UI → DAG → Clear all task instances
3. Trigger DAG manually

### Pre-commit checklist for new datasets
- [ ] MANIFEST.md updated
- [ ] schema_expected.yaml matches CSV columns
- [ ] create_table.sql matches schema YAML
- [ ] Sample CSV placed in sample_data/
- [ ] DAG tested end-to-end locally
- [ ] README updated
- [ ] .env.sample has no real credentials

