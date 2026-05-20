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
