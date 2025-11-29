# Airflow DAG Configurations

This repository contains all configuration files (YAML, SQL) for Airflow DAGs in the Glynac data platform.


## Directory Structure

```
airflow-dag-configs/
├── config/
│   ├── global_config.yaml              # Global settings (MinIO, ClickHouse)
│   ├── monitoring_config.yaml          # Monitoring configuration
│   ├── bronze/                         # Bronze layer configs
│   │   ├── redtail/
│   │   │   ├── call.yaml
│   │   │   └── account.yaml
│   │   └── salesforce/
│   │       └── contact.yaml
│   ├── silver/                         # Silver layer configs
│   │   └── {table_name}/
│   │       ├── dag.yaml                # DAG configuration
│   │       ├── schema.yaml             # Table schema
│   │       └── query.sql               # Transformation SQL
│   ├── gold/                           # Gold layer configs
│   │   └── {category}/
│   │       └── {table_name}/
│   │           ├── dag.yaml
│   │           ├── schema.yaml
│   │           ├── query.sql
│   │           └── test.yaml           # Data quality tests
│   └── stream-flink/                   # Flink streaming SQL jobs
│       └── {source}/
│           └── {entity}/
│               └── main-processing.sql
├── tests/                              # Config validation tests
└── .github/workflows/
    ├── validate-configs.yml            # Validate YAML/SQL syntax
    └── sync-to-minio.yml               # Sync to MinIO bucket
```

## Adding New Pipelines

### Bronze Layer (Raw Data Ingestion)

Create `config/bronze/{source}/{table}.yaml`:

```yaml
metadata:
  description: "Ingest raw data from source"
  owner: data-team

source:
  type: s3
  path: s3://raw-data/{source}/{table}/
  format: parquet

schema:
  columns:
    - name: id
      type: String
    - name: created_at
      type: DateTime

schedule_interval: "@hourly"
```

### Silver Layer (Transformation)

Create `config/silver/{table_name}/` folder with:

**dag.yaml**:
```yaml
dag:
  description: "Transform raw data"
  schedule_interval: "@daily"
  owner: data-team

dependencies:
  - bronze_source_table
```

**schema.yaml**:
```yaml
table_name: my_silver_table
database: silver
engine: MergeTree()
order_by: [id, created_at]

columns:
  - name: id
    type: String
  - name: name
    type: String
  - name: created_at
    type: DateTime
```

**query.sql**:
```sql
SELECT
    id,
    COALESCE(name, 'Unknown') AS name,
    created_at
FROM bronze.source_table
WHERE created_at >= '{{ ds }}'
```

### Gold Layer (Analytics)

Create `config/gold/{category}/{table_name}/` folder with same structure as silver.

### Flink Streaming Jobs

Create `config/stream-flink/{source}/{entity}/main-processing.sql`:

```sql
-- Kafka source table
CREATE TABLE source_kafka (
    id STRING,
    data STRING,
    event_time TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'my-topic',
    'properties.bootstrap.servers' = '{{KAFKA_BOOTSTRAP_SERVERS}}',
    'format' = 'json'
);

-- MinIO sink table
CREATE TABLE sink_s3 (
    id STRING,
    data STRING,
    event_time TIMESTAMP(3)
) WITH (
    'connector' = 'filesystem',
    'path' = '{{MINIO_S3_ENDPOINT}}/processed/',
    'format' = 'parquet'
);

-- Insert statement
INSERT INTO sink_s3
SELECT * FROM source_kafka;
```

## CI/CD Workflows

### validate-configs.yml
- Validates YAML syntax
- Validates SQL syntax
- Runs on all PRs

### sync-to-minio.yml
- Syncs `config/` folder to MinIO bucket
- Runs on push to `main` branch
- Uses `mc mirror` command

## GitHub Secrets Required

Set these in repository settings:

| Secret | Description |
|--------|-------------|
| `MINIO_ENDPOINT` | MinIO endpoint URL (e.g., `http://10.104.16.8:9000`) |
| `MINIO_ACCESS_KEY` | MinIO access key |
| `MINIO_SECRET_KEY` | MinIO secret key |
| `MINIO_CONFIG_BUCKET` | Bucket name (default: `airflow-configs`) |

## Local Testing

### Validate YAML Syntax

```bash
pip install pyyaml
python -c "import yaml; yaml.safe_load(open('config/bronze/redtail/call.yaml'))"
```

### Test MinIO Upload Manually

```bash
# Install mc
wget https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc

# Configure alias
./mc alias set minio http://10.104.16.8:9000 minioadmin minioadmin

# Test sync
./mc mirror config/ minio/airflow-configs/ --dry-run
```

## Related Repositories

| Repository | Purpose |
|------------|---------|
| `airflow-generated-dags` | DAG generators that read configs from MinIO |
| `data-platform-infrastructure` | Dockerfile, Nomad config |

## License

Proprietary - Glynac AI
