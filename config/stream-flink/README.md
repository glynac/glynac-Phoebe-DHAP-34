# Flink SQL Configuration

This directory contains Flink SQL streaming job configurations that are automatically deployed by the `flink_sql_auto_manager` DAG.

## 📁 Directory Structure

```
stream-flink/
├── README.md                          ← You are here
├── _template_example.sql              ← Example template with variables
├── redtail/
│   ├── account/main-processing.sql
│   ├── activity/main-processing.sql
│   └── asset/main-processing.sql
└── salesforce/
    ├── account/main-processing.sql
    └── campaign/main-processing.sql
```

## 🔐 Hiding Secrets with Template Variables

**IMPORTANT:** Never commit hardcoded IPs, endpoints, or credentials!

### Before (Hardcoded - BAD ❌)

```sql
CREATE TABLE my_source (...) WITH (
    'connector' = 'kafka',
    'properties.bootstrap.servers' = '167.172.66.204:9092',  -- ❌ Hardcoded!
    ...
);

CREATE TABLE my_sink (...) WITH (
    'connector' = 'filesystem',
    'path' = 's3a://main-data/my-data/',  -- ❌ Hardcoded!
    ...
);
```

### After (Template Variables - GOOD ✅)

```sql
CREATE TABLE my_source (...) WITH (
    'connector' = 'kafka',
    'properties.bootstrap.servers' = '{{KAFKA_BOOTSTRAP_SERVERS}}',  -- ✅ Variable!
    ...
);

CREATE TABLE my_sink (...) WITH (
    'connector' = 'filesystem',
    'path' = '{{MINIO_S3_ENDPOINT}}/my-data/',  -- ✅ Variable!
    ...
);
```

## 📝 Available Template Variables

Configure these in Airflow (Admin > Variables):

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `167.172.66.204:9092` | Kafka broker addresses |
| `MINIO_S3_ENDPOINT` | `s3a://main-data` | MinIO S3 endpoint |
| `MINIO_S3_BUCKET` | `main-data` | MinIO bucket name |

## 🔄 Migration Guide

### Step 1: Update Your SQL Files

Replace hardcoded values with template variables:

```bash
# Example for redtail/account/main-processing.sql
sed -i '' "s/'properties.bootstrap.servers' = '[^']*'/'properties.bootstrap.servers' = '{{KAFKA_BOOTSTRAP_SERVERS}}'/g" redtail/account/main-processing.sql

sed -i '' "s/'path' = 's3a:\/\/[^\/]*/'path' = '{{MINIO_S3_ENDPOINT}}/g" redtail/account/main-processing.sql
```

Or manually edit:
1. Find: `'properties.bootstrap.servers' = '167.172.66.204:9092'`
   Replace with: `'properties.bootstrap.servers' = '{{KAFKA_BOOTSTRAP_SERVERS}}'`

2. Find: `'path' = 's3a://main-data/`
   Replace with: `'path' = '{{MINIO_S3_ENDPOINT}}/`

### Step 2: Set Airflow Variables

In Airflow UI: **Admin > Variables** → Add:
```
KAFKA_BOOTSTRAP_SERVERS = 167.172.66.204:9092
MINIO_S3_ENDPOINT = s3a://main-data
MINIO_S3_BUCKET = main-data
```

Or via CLI:
```bash
airflow variables set KAFKA_BOOTSTRAP_SERVERS "167.172.66.204:9092"
airflow variables set MINIO_S3_ENDPOINT "s3a://main-data"
airflow variables set MINIO_S3_BUCKET "main-data"
```

### Step 3: Test

1. Commit updated SQL files
2. DAG will automatically pick up changes
3. New jobs will be submitted with substituted values
4. Check Airflow logs to verify substitution

## 🎯 Job Naming Convention

Jobs are named: `{source}-{entity}`

Examples:
- `config/stream-flink/redtail/account/` → Job: `redtail-account`
- `config/stream-flink/salesforce/campaign/` → Job: `salesforce-campaign`

## 🚀 Adding a New Job

1. Create directory: `config/stream-flink/{source}/{entity}/`
2. Add SQL file: `main-processing.sql` (use template variables!)
3. Commit and push
4. Job will auto-deploy within 5 minutes

## 🗑️ Removing a Job

1. Delete directory or SQL file
2. Commit and push
3. Job will auto-stop within 5 minutes

## 📚 Example

See [_template_example.sql](_template_example.sql) for a complete example with template variables.

## ⚠️ Important Notes

- **Always use template variables** for IPs, endpoints, credentials
- **Never commit secrets** to Git
- SQL files are read at runtime, so changes take effect immediately
- Template supports both `{{VAR}}` and `${VAR}` syntax
- One folder = One Flink job (even with multiple SQL statements)
