# Data Engineering Internship Report
## Story 1: Email Thread Summary Dataset Pipeline

**Project:** DHAP-34  
**Date:** April 26, 2026  
**Prepared by:** Sanchit Borikar  
**Report Type:** Technical Documentation

---

## 1. Introduction

This report documents the completion of **Story 1** of the DHAP-34 data engineering project. The story focuses on building a foundational data pipeline by ingesting, validating, and preparing email thread data for downstream loading into PostgreSQL via Apache Airflow. The work demonstrates core data engineering practices including data preprocessing, schema design, and infrastructure setup for production-ready data pipelines.

---

## 2. Objective of the Project

**Primary Objective:**
Establish a structured, validated dataset pipeline that merges multiple email data sources and prepares them for ingestion into a PostgreSQL database managed by Apache Airflow.

**Specific Goals for Story 1:**
- Ingest and validate two CSV datasets from Kaggle
- Merge datasets on a common key (thread_id) using an inner join strategy
- Standardize data types and handle SQL reserved keywords
- Create schema contracts (YAML and SQL DDL) that define the target table structure
- Establish environment configuration templates for reproducibility
- Generate comprehensive dataset metadata and documentation

**Success Criteria:**
- Unified dataset with 21,684 records (matching inner join cardinality)
- Schema compliance with null safety and type constraints
- All SQL reserved keywords properly renamed
- PostgreSQL-ready DDL generated and tested
- Complete project structure with config and sample data folders

---

## 3. Dataset Description

### Source Data

**Dataset Name:** Email Thread Summary Dataset (Kaggle)

**Source Files:**
1. **email_thread_details.csv**
   - Records: 21,684 email messages
   - Columns: thread_id, subject, timestamp, from, to, body
   - Purpose: Contains raw email metadata and content
   - Data Type Profile: 1 integer, 5 text fields

2. **email_thread_summary.csv**
   - Records: 4,167 thread summaries
   - Columns: thread_id, summary
   - Purpose: Contains AI-generated or human-written thread summaries
   - Data Type Profile: 1 integer, 1 text field

### Join Strategy

- **Join Type:** INNER JOIN
- **Join Key:** thread_id
- **Rationale:** Inner join ensures only threads with complete details AND summaries are included, eliminating orphaned records and ensuring data quality
- **Result Cardinality:** 21,684 rows (all detail rows had corresponding summaries)

### Dataset Characteristics

- **Total Records:** 21,684 email threads with summaries
- **Total Columns:** 8 (after transformation)
- **Data Volume:** ~50 MB (final_dataset.csv)
- **Time Span:** Email threads spanning multiple years
- **Domain:** Corporate email communication dataset

---

## 4. Data Preprocessing and Transformation

### Preprocessing Steps

**Step 1: Data Loading**
```python
df1 = pd.read_csv("email_thread_details.csv")  # 21,684 rows
df2 = pd.read_csv("email_thread_summary.csv")  # 4,167 rows
```
- Both files loaded into pandas DataFrames with default UTF-8 encoding
- No missing values detected in initial inspection

**Step 2: Data Merging**
```python
merged = df1.merge(df2, on="thread_id", how="inner")
```
- Inner join on thread_id preserved data integrity
- All 21,684 detail records matched with summary records
- Result: 21,684 x 8 DataFrame

**Step 3: Synthetic Primary Key Generation**
```python
merged.reset_index(inplace=True)
merged.rename(columns={"index": "id"}, inplace=True)
```
- Created auto-incremented id column (0-21683)
- Rationale: Ensures unique row identification for database operations
- Type: INTEGER NOT NULL

**Step 4: Reserved Keyword Handling**
```python
merged.rename(columns={
    "from": "from_email",
    "to": "to_email"
}, inplace=True)
```
- Problem: "from" and "to" are SQL reserved keywords
- Solution: Renamed to "from_email" and "to_email"
- Impact: Prevents SQL syntax errors during CREATE TABLE and INSERT operations

**Step 5: Timestamp Conversion**
```python
merged['timestamp'] = pd.to_datetime(merged['timestamp'], errors='coerce')
```
- Converted timestamp column from string (object) to datetime64[ns]
- Error handling: 'coerce' mode converts unparseable dates to NaT
- Result: Proper temporal data type for time-series analysis

### Final Dataset Schema

| Column | Type | Nullable | Source | Purpose |
|--------|------|----------|--------|---------|
| id | INTEGER | NO | Synthetic | Primary key for row identification |
| thread_id | INTEGER | NO | email_thread_details | Unique thread identifier |
| subject | TEXT | NO | email_thread_details | Email subject line |
| timestamp | TIMESTAMP | YES | email_thread_details | Date/time of email |
| from_email | TEXT | NO | email_thread_details | Sender email address |
| to_email | TEXT | NO | email_thread_details | Recipient email address |
| body | TEXT | NO | email_thread_details | Email message content |
| summary | TEXT | YES | email_thread_summary | AI/human-generated thread summary |

---

## 5. Schema Design and Data Modeling

### Data Modeling Approach

**Design Pattern:** Denormalized Fact Table

**Rationale:**
- Simplifies data loading pipeline (single table vs. normalized multi-table join)
- Reduces complexity for Airflow DAG design
- Optimizes read performance for downstream analytics queries
- Maintains data lineage and thread context in a single row

### Schema Contract (YAML)

The schema is defined in `schema_expected.yaml` using a declarative YAML format:

```yaml
table: public.email_thread_summary
primary_key: [id]
columns:
  - name: id
    type: integer
    nullable: false
  - name: thread_id
    type: integer
    nullable: false
  - name: subject
    type: text
    nullable: false
  - name: timestamp
    type: timestamp
    nullable: true
  - name: from_email
    type: text
    nullable: false
  - name: to_email
    type: text
    nullable: false
  - name: body
    type: text
    nullable: false
  - name: summary
    type: text
    nullable: true
```

**Key Design Decisions:**

1. **Primary Key:** Single column (id) for simplicity and database constraint enforcement
2. **Nullable Fields:**
   - timestamp: NULLABLE (null coercion during datetime conversion may produce NaT values)
   - summary: NULLABLE (some threads may lack summaries)
   - All other fields: NOT NULL (business requirement for data quality)
3. **Text Fields:** Used TEXT type for unlimited-length email content and summaries
4. **Temporal Field:** TIMESTAMP type enables time-series analysis and audit trails

### SQL DDL

PostgreSQL DDL in `create_table.sql`:

```sql
CREATE TABLE IF NOT EXISTS public.email_thread_summary (
id INTEGER NOT NULL,
thread_id INTEGER NOT NULL,
subject TEXT NOT NULL,
timestamp TIMESTAMP NULL,
from_email TEXT NOT NULL,
to_email TEXT NOT NULL,
body TEXT NOT NULL,
summary TEXT NULL,
PRIMARY KEY (id)
);
```

**Design Rationale:**
- IF NOT EXISTS: Idempotent—safe to run multiple times without errors
- Schema Namespace: public schema (default PostgreSQL location)
- Constraints: Primary key ensures no duplicate rows
- Type Alignment: Matches pandas DataFrame dtypes for seamless data loading

---

## 6. Files and Project Structure Explanation

### Project Structure

```
extraction/email_thread_summary/
├── MANIFEST.md
├── .env.sample
├── config/
│   ├── schema_expected.yaml
│   └── create_table.sql
└── sample_data/
    ├── final_dataset.csv
    ├── email_thread_details.csv
    ├── email_thread_summaries.csv
    └── analyze.py
```

### File Descriptions

#### 1. MANIFEST.md
**Purpose:** Dataset metadata and documentation
**Contents:**
- Dataset name and description
- Data source attribution (Kaggle)
- Local file path for version control
- Target table specification
- File format (CSV)
- Data transformation notes

**Engineering Value:** Provides data lineage and context for downstream team members; enables reproducibility and audit trails.

#### 2. schema_expected.yaml
**Purpose:** Schema contract defining table structure
**Contents:**
- Target table: `public.email_thread_summary`
- Primary key: `[id]`
- 8 column definitions with name, type, and nullability constraints

**Engineering Value:** 
- Machine-readable schema enables automated validation
- Enables data quality checks before database loading
- Serves as documentation for Airflow tasks
- Supports schema evolution tracking

#### 3. create_table.sql
**Purpose:** PostgreSQL DDL for table creation
**Contents:** CREATE TABLE statement matching schema_expected.yaml

**Engineering Value:**
- Production-ready SQL for database initialization
- Ensures schema consistency between YAML contract and actual database
- Can be run in Airflow via PostgresOperator
- Supports infrastructure-as-code practices

#### 4. .env.sample
**Purpose:** Environment variable template
**Contents:**
```
PG_HOST=localhost
PG_PORT=5432
PG_DB=airflow_db
PG_USER=airflow_user
PG_PASSWORD=airflow_pass
```

**Engineering Value:**
- Enables local/dev/prod environment configuration
- Separates secrets from code (security best practice)
- Users create .env from .env.sample and customize for their environment
- Supports 12-factor app methodology

#### 5. final_dataset.csv
**Purpose:** Merged, validated, and transformed dataset
**Contents:** 21,684 rows × 8 columns of email thread data
**File Size:** ~50 MB

**Engineering Value:**
- Ready for PostgreSQL COPY/INSERT operations
- Serves as test/sample data for Airflow development
- Can be versioned in Git LFS for reproducibility
- Enables data validation checks before pipeline execution

#### 6. analyze.py
**Purpose:** Data preprocessing and transformation script
**Contents:** Pandas-based ETL logic for merge, rename, and type conversion

**Engineering Value:**
- Demonstrates reproducible data transformation
- Can be adapted for production Airflow tasks
- Documents transformation logic for team reference
- Enables rapid iteration during development

---

## 7. Challenges Faced and Solutions

### Challenge 1: SQL Reserved Keywords

**Problem:**
- Columns named "from" and "to" are SQL reserved keywords
- Attempting `CREATE TABLE` with these names would fail with syntax error

**Solution:**
- Renamed columns during pandas transformation:
  - from → from_email
  - to → to_email
- Updated MANIFEST.md to document the renaming decision
- Ensured consistency across schema_expected.yaml and create_table.sql

**Learning:** Always validate column names against database-specific reserved word lists during schema design.

---

### Challenge 2: Timestamp Data Type Handling

**Problem:**
- Timestamp column stored as string (object type) in CSV
- PostgreSQL requires explicit TIMESTAMP type
- Unparseable date strings could cause database load failures

**Solution:**
- Applied `pd.to_datetime(merged['timestamp'], errors='coerce')`
- Coercion mode converts unparseable values to NaT (missing)
- Marked timestamp column as NULLABLE in schema
- Documented the approach in MANIFEST.md

**Learning:** Handle type conversion explicitly during ETL; use coercion modes strategically to balance data quality with failure resilience.

---

### Challenge 3: Join Cardinality Verification

**Problem:**
- Two datasets with different record counts (21,684 vs 4,167)
- Unknown join key distribution; potential for data loss

**Solution:**
- Performed inner join and verified result cardinality
- Result: 21,684 rows (all detail rows had summaries)
- No orphaned records in details dataset
- Documented join strategy and outcome in analysis

**Learning:** Always verify join outcomes; mismatches indicate data quality issues that require investigation.

---

### Challenge 4: Git Repository Access

**Problem:**
- External GitHub repository not accessible from current environment
- Prevented pushing Story 1 files to shared collaboration repo

**Workaround:**
- Created local project structure at D:\DHAP34\intern-project\Sanchit_Borikar\project-DHAP-34\
- All Story 1 files ready for push once repository access is restored
- Complete documentation enables handoff to team

**Learning:** Implement local version control workflows to proceed with work while awaiting infrastructure access.

---

## 8. Key Learnings

### 1. Data Preprocessing is Critical
- 80% of data engineering effort goes to preprocessing and validation
- Early detection of data quality issues (types, nulls, reserved keywords) prevents downstream failures
- Systematic approach to transformation increases pipeline reliability

### 2. Schema Design Drives Pipeline Architecture
- Schema contracts (YAML + SQL) define the interface between stages
- Clear nullability constraints prevent ambiguity in data loading
- Denormalized designs simplify pipeline complexity for smaller datasets

### 3. Documentation is Engineering
- MANIFEST.md, schema_expected.yaml, and .env.sample are code
- Machine-readable formats (YAML) enable automation
- Clear documentation reduces onboarding friction for team members

### 4. Reserved Keywords Require Discipline
- SQL dialects have different keyword lists
- Column naming conventions should avoid reserved words upstream
- Rename early during ETL to prevent downstream errors

### 5. Test Data Enables Rapid Iteration
- final_dataset.csv serves as test data for Airflow development
- Allows schema testing and transformation logic validation without hitting production database
- Enables local development without database access

### 6. Environment Configuration Enables Portability
- .env.sample approach separates configuration from code
- Enables local dev, staging, and production environments
- Reduces friction when code moves between environments

---

## 9. Conclusion

### Completion Status

Story 1 has been **successfully completed** with all deliverables meeting technical requirements:

✓ Dataset merged and validated (21,684 records)  
✓ Schema contract created (schema_expected.yaml)  
✓ PostgreSQL DDL generated (create_table.sql)  
✓ Metadata documented (MANIFEST.md)  
✓ Environment template provided (.env.sample)  
✓ Sample dataset prepared (final_dataset.csv)  
✓ Data transformation logic documented (analyze.py)  

### Technical Quality

The work demonstrates strong data engineering fundamentals:
- **Data Quality:** Null handling, type conversion, reserved keyword management
- **Schema Design:** Clear constraints, denormalized approach appropriate for pipeline stage
- **Documentation:** Machine-readable contracts and comprehensive metadata
- **Reproducibility:** Environment configuration and script-based transformation

### Next Steps (Story 2 Roadmap)

This Story 1 foundation enables the following Story 2 activities:
1. **Airflow DAG Development:** Create `email_thread_summary_etl.py` DAG using schema contract
2. **Data Validation:** Implement Great Expectations checks against schema_expected.yaml
3. **PostgreSQL Integration:** Deploy create_table.sql and COPY final_dataset.csv via Airflow
4. **Monitoring:** Add data quality metrics and pipeline health checks
5. **Incremental Loading:** Design upsert logic for handling new email threads

### Professional Outcomes

This project demonstrates competency in:
- **Data Engineering:** End-to-end ETL pipeline thinking
- **Software Engineering:** Infrastructure-as-code, environment configuration, version control
- **Technical Communication:** Clear documentation and schema contracts
- **Problem-Solving:** Handling real-world data challenges (reserved keywords, type conversions)

---

## Appendix: Command Reference

### Environment Setup
```bash
cd d:\DHAP34\extraction\email_thread_summary\sample_data
python analyze.py
```

### Schema Validation
```sql
-- Deploy schema
psql -h localhost -U airflow_user -d airflow_db -f config/create_table.sql

-- Verify table creation
\dt public.email_thread_summary
```

### Data Loading (Airflow-ready)
```sql
COPY public.email_thread_summary FROM '/path/to/final_dataset.csv' WITH (FORMAT CSV, HEADER);
```

---

**Report End**  
**Status:** Ready for Internship Submission  
**Version:** 1.0  
**Last Updated:** April 26, 2026
