# Chatbot Conversations — Airflow Pipeline

## Dataset
3K Conversations Dataset for ChatBot — question and answer pairs for chatbot training.
Target table: `public.chatbot_conversations`

## Prerequisites
- Docker + Docker Compose installed
- Python 3.9+

## Setup

### 1. Clone the repo
```bash
git clone https://github.com/glynac/glynac-DHAP-34.git
cd glynac-DHAP-34
```

### 2. Configure environment variables
```bash
cp intern-project/Aadya-Anil-Kumar/project-DHAP-34/chatbot-conversations/.env.example .env
```
Edit `.env` and fill in your PostgreSQL credentials.

### 3. Start the environment
```bash
docker compose up -d
```

### 4. Access Airflow UI
Open http://localhost:8081 in your browser.
- Username: `airflow`
- Password: `airflow`

### 5. Stop the environment
```bash
docker compose down
```

## DAG
The pipeline DAG is located at:
`extraction/chatbot-conversations/dags/chatbot_conversations_ingest.py`

### Task Flow
file_check → validate_schema → transform → load_to_postgres

### Task Details
- **file_check** — Verifies CSV exists in `sample_data/`
- **validate_schema** — Compares CSV columns against `config/schema_expected.yaml`
- **transform** — Strips whitespace, handles nulls
- **load_to_postgres** — Inserts clean data into `public.chatbot_conversations`

## Environment Variables
See `.env.example` for all required variables:

| Variable | Description |
|----------|-------------|
| EXT_PG_HOST | PostgreSQL host |
| EXT_PG_PORT | PostgreSQL port (default 5432) |
| EXT_PG_DB | Database name |
| EXT_PG_USER | Database user |
| EXT_PG_PASSWORD | Database password |
| EXT_PG_SSLMODE | SSL mode (default prefer) |

## Troubleshooting

**Schema mismatch error**
Check that your CSV columns match `config/schema_expected.yaml` exactly.

**Missing CSV error**
Make sure your CSV is in `extraction/chatbot-conversations/sample_data/`.

**Invalid credentials**
Double check your `.env` file has the correct PostgreSQL credentials.

**Reset a DAG run**
In Airflow UI → DAGs → chatbot_conversations_ingest → Clear all tasks.
