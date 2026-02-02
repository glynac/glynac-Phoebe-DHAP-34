# Organization 123 - Canonical Event Timeline

**Org ID:** `29a436a3-b5de-4afd-9c7a-059246c5a681`
**Database:** `org_123`
**Timeline Table:** `org_123.timeline`
**Status:** Active

---

## Overview

Organization 123 is the pilot implementation of the Canonical Event Timeline. It combines events from 5 Redtail source tables into a single unified timeline.

### Current Event Sources

| Source Table | Event Type | Status |
|--------------|------------|--------|
| `redtail_silver.account` | `account_created` | Active |
| `redtail_silver.activity` | `activity_logged` | Active |
| `redtail_silver.call` | `call_made` | Active |
| `redtail_silver.client` | `client_created` | Active |
| `redtail_silver.communication` | `communication_logged` | Active |

---

## Directory Structure

```
org_123/
├── README.md                      ← You are here
├── dag.yaml                       ← ONE DAG config with TaskGroups
├── base_timeline/
│   └── table.sql                  ← CREATE DATABASE org_123 + CREATE TABLE timeline
├── mv_redtail_account/
│   └── mv.sql                     ← MV: account → timeline
├── mv_redtail_activity/
│   └── mv.sql                     ← MV: activity → timeline
├── mv_redtail_call/
│   └── mv.sql                     ← MV: call → timeline
├── mv_redtail_client/
│   └── mv.sql                     ← MV: client → timeline
└── mv_redtail_communication/
    └── mv.sql                     ← MV: communication → timeline
```

---

## DAG Structure

**DAG ID:** `silver__timeline_org_123`
**Schedule:** `@once` (one-time setup)

```
silver__timeline_org_123
│
├── base_timeline (TaskGroup)
│   ├── create_table     → Creates org_123 database + timeline table
│   └── validate_table   → Verifies table exists
│
├── mv_redtail_account (TaskGroup)
│   ├── create_mv        → Creates MV + backfills existing data
│   └── validate_mv      → Verifies events inserted
│
├── mv_redtail_activity (TaskGroup)
│   ├── create_mv
│   └── validate_mv
│
├── mv_redtail_call (TaskGroup)
│   ├── create_mv
│   └── validate_mv
│
├── mv_redtail_client (TaskGroup)
│   ├── create_mv
│   └── validate_mv
│
└── mv_redtail_communication (TaskGroup)
    ├── create_mv
    └── validate_mv
```

**Dependencies:**
- All MV TaskGroups depend on `base_timeline` completing first
- MV TaskGroups can run in parallel after base_timeline

---

## Schema

### Timeline Table Columns

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | UUID | Unique event identifier |
| `org_id` | String | Organization UUID (`29a436a3-b5de-4afd-9c7a-059246c5a681`) |
| `event_type` | LowCardinality(String) | Event type (e.g., `account_created`) |
| `timestamp` | DateTime64(3) | When event occurred |
| `entity_type` | LowCardinality(String) | Entity type (e.g., `account`) |
| `entity_id` | String | Entity identifier (e.g., `account_1`) |
| `description` | String | Human-readable description |
| `source_system` | LowCardinality(String) | Source system (`redtail`) |
| `source_table` | String | Source table name |
| `source_id` | String | Original record ID |
| `minio_path` | Nullable(String) | Document path (future use) |
| `rec_add` | Nullable(DateTime64(3)) | When created in source |
| `rec_edit` | Nullable(DateTime64(3)) | When last edited in source |
| `metadata` | Nullable(String) | JSON with extra fields |
| `processing_date` | Date | Partition key |
| `_loaded_at` | DateTime | When inserted to timeline |
| `_version` | Int32 | Version for deduplication |

---

## Validation Queries

### 1. Event Count by Type

```sql
SELECT
    event_type,
    count() as event_count,
    min(timestamp) as earliest,
    max(timestamp) as latest
FROM org_123.timeline
GROUP BY event_type
ORDER BY event_count DESC;
```

### 2. Full Timeline (Recent)

```sql
SELECT
    timestamp,
    event_type,
    entity_id,
    description,
    rec_add,
    rec_edit
FROM org_123.timeline
ORDER BY timestamp DESC
LIMIT 50;
```

### 3. Check Source Distribution

```sql
SELECT
    source_table,
    event_type,
    count() as cnt
FROM org_123.timeline
GROUP BY source_table, event_type
ORDER BY source_table;
```

### 4. Verify Audit Timestamps

```sql
SELECT
    event_type,
    count() as total,
    countIf(rec_add IS NOT NULL) as has_rec_add,
    countIf(rec_edit IS NOT NULL) as has_rec_edit
FROM org_123.timeline
GROUP BY event_type;
```

### 5. Recent Changes (by rec_edit)

```sql
SELECT
    entity_id,
    event_type,
    description,
    rec_add as created,
    rec_edit as last_modified
FROM org_123.timeline
WHERE rec_edit IS NOT NULL
ORDER BY rec_edit DESC
LIMIT 20;
```

---

## Materialized Views

### How MVs Work

Each MV watches a source table and automatically inserts into the timeline:

```sql
-- Example: mv_redtail_account_to_timeline
CREATE MATERIALIZED VIEW org_123.mv_redtail_account_to_timeline
TO org_123.timeline    ← Target table
AS
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'account_created' AS event_type,
    ...
    rec_add,           ← Audit timestamp: when created
    rec_edit,          ← Audit timestamp: when edited
    ...
FROM redtail_silver.account    ← Source table
WHERE glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681';
```

### List All MVs

```sql
SELECT name, engine, as_select
FROM system.tables
WHERE database = 'org_123'
  AND engine = 'MaterializedView';
```

---

## Cleanup (If Needed)

To completely reset and recreate:

```sql
-- Drop all MVs first
DROP VIEW IF EXISTS org_123.mv_redtail_account_to_timeline;
DROP VIEW IF EXISTS org_123.mv_redtail_activity_to_timeline;
DROP VIEW IF EXISTS org_123.mv_redtail_call_to_timeline;
DROP VIEW IF EXISTS org_123.mv_redtail_client_to_timeline;
DROP VIEW IF EXISTS org_123.mv_redtail_communication_to_timeline;

-- Drop timeline table
DROP TABLE IF EXISTS org_123.timeline;

-- Optionally drop database
DROP DATABASE IF EXISTS org_123;
```

Then re-run the DAG: `airflow dags trigger silver__timeline_org_123`

---

## Adding More Event Sources

To add a new source table (e.g., `redtail_silver.note`):

### 1. Create MV folder

```bash
mkdir mv_redtail_note
```

### 2. Create mv.sql

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS org_123.mv_redtail_note_to_timeline
TO org_123.timeline
AS
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'note_added' AS event_type,
    COALESCE(note_date, rec_add, processing_timestamp, now()) AS timestamp,
    'note' AS entity_type,
    concat('note_', toString(rec_id)) AS entity_id,
    concat('Note: ', COALESCE(subject, 'No subject')) AS description,
    'redtail' AS source_system,
    'redtail_silver.note' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add,
    rec_edit,
    toJSONString(map(...)) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.note
WHERE rec_id IS NOT NULL
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681';
```

### 3. Add to dag.yaml

Add new component in the `components` section.

### 4. Sync and run

```bash
# Sync to MinIO
mc mirror --overwrite config/ myminio/airflow-configs/config/

# Re-run DAG
airflow dags trigger silver__timeline_org_123
```

---

## Support

**Questions?** Contact Data Engineering team.
