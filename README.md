# Airflow DAG Configurations

This repository contains all configuration files for Airflow DAGs in the Glynac data platform. It is part of a multi-repository architecture that separates configuration, code, and generated DAGs.

## Repository Purpose

This repository contains ONLY configuration files (YAML and SQL). No Python code or DAG definitions are stored here. DAG files are automatically generated from these configurations by the [airflow-generated-dags](https://github.com/Glynac-AI/airflow-generated-dags) repository using the [airflow-shared-components](https://github.com/Glynac-AI/airflow-shared-components) package.

## Directory Structure

```
airflow-dag-configs/
├── config/
│   ├── global_config.yaml          # Global Airflow settings (MinIO, ClickHouse, etc.)
│   ├── monitoring_config.yaml      # Monitoring and alerting configuration
│   ├── bronze/                     # Bronze layer configs
│   │   ├── redtail/               # Redtail source configs
│   │   └── salesforce/            # Salesforce source configs
│   ├── silver/                     # Silver layer transformation configs
│   │   └── salesforce_user/       # Example: Salesforce user transformation
│   │       ├── dag.yaml           # DAG configuration
│   │       ├── schema.yaml        # Table schema
│   │       ├── query.sql          # Transformation SQL
│   │       └── tests.yaml         # Data quality tests
│   ├── gold/                       # Gold layer analytics configs
│   │   ├── dimensions/            # Dimension tables
│   │   └── metrics/               # Metric tables
│   └── stream-flink/              # Flink streaming SQL jobs
│       ├── salesforce/            # Salesforce streaming jobs
│       └── redtail/               # Redtail streaming jobs
├── tests/                          # Configuration validation tests
├── .github/workflows/              # CI/CD pipelines
└── README.md                       # This file
```

## Configuration Types

### Bronze Layer
- **Purpose**: Ingest raw data from sources (Salesforce, Redtail, etc.)
- **Files**: One YAML per table
- **Location**: `config/bronze/{source}/{table}.yaml`
- **Example**: `config/bronze/salesforce/account.yaml`

### Silver Layer
- **Purpose**: Transform and clean Bronze data
- **Files**:
  - `dag.yaml` - DAG configuration (schedule, dependencies)
  - `schema.yaml` - Target table schema
  - `query.sql` - Transformation SQL
  - `tests.yaml` - Data quality tests
- **Location**: `config/silver/{table_name}/`
- **Example**: `config/silver/salesforce_user/`

### Gold Layer
- **Purpose**: Business-ready analytics tables (dimensions & metrics)
- **Files**: Same as Silver layer
- **Location**: `config/gold/{dimensions|metrics}/{table_name}/`
- **Types**:
  - **Dimensions**: Slowly changing dimensions (e.g., dim_account, dim_user)
  - **Metrics**: Aggregated metrics (e.g., user_daily_metrics, revenue_metrics)

### Flink Streaming
- **Purpose**: Real-time data processing with Flink SQL
- **Files**: `main-processing.sql` - Flink SQL job definition
- **Location**: `config/stream-flink/{source}/{table}/main-processing.sql`
- **Example**: `config/stream-flink/salesforce/account/main-processing.sql`

## Configuration Schema

### Bronze Layer YAML
```yaml
table_name: account
source_system: salesforce
object_name: Account
extraction_mode: incremental  # or: full
incremental_field: SystemModstamp
schedule: "0 */6 * * *"
```

### Silver Layer DAG Configuration
```yaml
table_name: salesforce_user
source_tables:
  - bronze_salesforce_user
schedule: "0 1 * * *"
depends_on: []
tags:
  - silver
  - salesforce
```

### Silver Layer Schema
```yaml
columns:
  - name: user_id
    type: String
    description: Unique user identifier
  - name: email
    type: String
    description: User email address
  - name: created_date
    type: DateTime
    description: User creation timestamp
```

### Silver Layer Transformation SQL
```sql
SELECT
    Id as user_id,
    Email as email,
    CreatedDate as created_date,
    LastModifiedDate as last_modified_date
FROM bronze.salesforce_user
WHERE is_deleted = false
```

## Making Changes

### For Data Analysts

1. **Clone the repository**:
   ```bash
   git clone git@github-glynac:Glynac-AI/airflow-dag-configs.git
   cd airflow-dag-configs
   ```

2. **Create a new branch**:
   ```bash
   git checkout -b feature/add-new-metric
   ```

3. **Make your changes**:
   - Edit existing configs or create new ones
   - Follow the directory structure conventions
   - Ensure YAML syntax is valid

4. **Test locally** (optional):
   ```bash
   cd tests
   pip install -r requirements.txt
   pytest -v
   ```

5. **Commit and push**:
   ```bash
   git add .
   git commit -m "Add new metric: customer_lifetime_value"
   git push origin feature/add-new-metric
   ```

6. **Create a Pull Request**:
   - Go to GitHub and create a PR
   - CI/CD will automatically validate your configs
   - Once approved and merged, DAGs will be auto-generated

### Adding a New Bronze Table

1. Create a new YAML file in `config/bronze/{source}/`:
   ```yaml
   table_name: my_new_table
   source_system: salesforce
   object_name: MyObject__c
   extraction_mode: incremental
   incremental_field: LastModifiedDate
   schedule: "0 */4 * * *"
   ```

2. Commit and push - that's it! The DAG will be auto-generated.

### Adding a New Silver Table

1. Create a directory: `config/silver/my_new_table/`
2. Add four files:
   - `dag.yaml` - DAG configuration
   - `schema.yaml` - Table schema
   - `query.sql` - Transformation SQL
   - `tests.yaml` - Data quality tests
3. Commit and push - the DAG will be auto-generated.

### Adding a New Gold Metric

1. Create a directory: `config/gold/metrics/my_new_metric/`
2. Add files:
   - `dag.yaml` - DAG configuration
   - `schema.yaml` - Table schema
   - `transform.py` or `query.sql` - Transformation logic
   - `tests.yaml` - Data quality tests
3. Commit and push - the DAG will be auto-generated.

## CI/CD Pipeline

When you push changes to this repository:

1. **Validation** runs automatically:
   - YAML syntax validation
   - SQL syntax checks
   - Schema validation
   - Structure tests

2. **If validation passes**:
   - Changes are ready to merge

3. **After merge to main**:
   - The [airflow-generated-dags](https://github.com/Glynac-AI/airflow-generated-dags) repository is notified
   - DAGs are automatically regenerated
   - New DAGs appear in Airflow within 3-5 minutes

## Validation

All configs are validated automatically in CI/CD:

```bash
# Run validation locally
cd tests
pytest test_config_validation.py -v
pytest test_bronze_configs.py -v
pytest test_silver_configs.py -v
```

## Best Practices

1. **Use descriptive names**: Table names should be clear and follow naming conventions
2. **Document your SQL**: Add comments to complex queries
3. **Define data quality tests**: Every table should have tests
4. **Use incremental extraction**: When possible, use incremental mode for Bronze tables
5. **Set appropriate schedules**: Consider data freshness needs and source system load
6. **Version control everything**: Never edit configs directly in production

## Troubleshooting

### YAML Syntax Error
- Use a YAML validator online or in your editor
- Check indentation (use spaces, not tabs)
- Ensure all strings with special characters are quoted

### SQL Syntax Error
- Test your SQL query in ClickHouse directly first
- Check for Jinja template syntax if using templating
- Ensure all referenced tables exist

### DAG Not Appearing
- Check CI/CD pipeline status on GitHub Actions
- Verify configs passed validation
- Wait 3-5 minutes for gitsync to pull changes
- Check Airflow logs for import errors

## Related Repositories

- **airflow-shared-components**: Python package with operators, hooks, and DAG generators
- **airflow-generated-dags**: Generated DAG files and custom DAGs
- **data-platform-infrastructure**: Infrastructure as Code (Terraform, Nomad)

## Support

For questions or issues:
- Create an issue in this repository
- Contact the data platform team
- Check the documentation in the `Docs/` directory

## License

Proprietary - Glynac AI
