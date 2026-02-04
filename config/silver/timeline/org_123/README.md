# Organization 123 - Canonical Event Timeline

**Org ID:** `29a436a3-b5de-4afd-9c7a-059246c5a681`
**Database:** `org_123`
**Timeline Table:** `org_123.timeline`
**Status:** Active
**Last Updated:** 2026-02-04

---

## Overview

Organization 123 is the pilot implementation of the Canonical Event Timeline. It combines events from 6 Redtail source tables into a single unified timeline using the **two-phase strategy** (backfill + ongoing).

### Current Event Sources

| Source Table | Event Types | Events/Record | Status |
|--------------|-------------|---------------|--------|
| `redtail_silver.account` | `account_created`, `account_opened`, `account_updated` | 3 | Active |
| `redtail_silver.activity` | `activity_created`, `activity_logged`, `activity_updated` | 3 | Active |
| `redtail_silver.call` | `call_created`, `call_made`, `call_updated` | 3 | Active |
| `redtail_silver.client` | `client_created`, `client_updated` | 2 | Active* |
| `redtail_silver.communication` | `communication_created`, `communication_logged`, `communication_updated` | 3 | Active |
| `redtail_silver.email` | `email_created`, `email_sent`, `email_updated` | 3 | Active |

*Note: `client_onboarded` event skipped due to missing `client_since` column in actual table (schema drift).

---

## Directory Structure

```
org_123/
├── README.md                      ← You are here
├── dag.yaml                       ← DAG configuration (7 components)
├── base_timeline/
│   └── table.sql                  ← CREATE DATABASE + TABLE + INDEXES
├── mv_redtail_account/
│   └── mv.sql                     ← 3 events per record
├── mv_redtail_activity/
│   └── mv.sql                     ← 3 events per record
├── mv_redtail_call/
│   └── mv.sql                     ← 3 events per record
├── mv_redtail_client/
│   └── mv.sql                     ← 2 events (client_since missing)
├── mv_redtail_communication/
│   └── mv.sql                     ← 3 events per record
└── mv_redtail_email/
    └── mv.sql                     ← 3 events per record
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
│   └── validate_mv      → Verifies events >= 1
│
├── mv_redtail_activity (TaskGroup)
├── mv_redtail_call (TaskGroup)
├── mv_redtail_client (TaskGroup)
├── mv_redtail_communication (TaskGroup)
└── mv_redtail_email (TaskGroup)

Dependencies: All MV TaskGroups depend on base_timeline
```

---

## Schema

### Timeline Table Columns

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | UUID | Unique event identifier |
| `org_id` | String | Organization UUID |
| `event_type` | LowCardinality(String) | e.g., `account_created`, `call_made` |
| `timestamp` | DateTime64(3) | When event occurred |
| `event_timestamp_source` | LowCardinality(String) | Source field: `rec_add`, `open_date`, etc. |
| `entity_type` | LowCardinality(String) | e.g., `account`, `call` |
| `entity_id` | String | e.g., `account_1`, `call_5` |
| `description` | String | Human-readable description |
| `source_system` | LowCardinality(String) | `redtail` |
| `source_table` | String | e.g., `redtail_silver.account` |
| `source_id` | String | Original `rec_id` |
| `minio_path` | Nullable(String) | Document path (future use) |
| `rec_add` | Nullable(DateTime64(3)) | When created in source |
| `rec_edit` | Nullable(DateTime64(3)) | When last edited in source |
| `metadata` | Nullable(String) | JSON with extra fields |
| `processing_date` | Date | Partition key |
| `_loaded_at` | DateTime | When inserted to timeline |
| `_version` | Int32 | Version for deduplication |

---

## Event Types Detail

### Account Events
| Event Type | Timestamp Source | Description |
|------------|------------------|-------------|
| `account_created` | `rec_add` | Record created in Redtail |
| `account_opened` | `open_date` | Account opening date (business event) |
| `account_updated` | `rec_edit` | Record modified |

### Activity Events
| Event Type | Timestamp Source | Description |
|------------|------------------|-------------|
| `activity_created` | `rec_add` | Activity record created |
| `activity_logged` | `activity_date` | When activity occurred (business event) |
| `activity_updated` | `rec_edit` | Activity record modified |

### Call Events
| Event Type | Timestamp Source | Description |
|------------|------------------|-------------|
| `call_created` | `rec_add` | Call record created |
| `call_made` | `call_date` | When call occurred (business event) |
| `call_updated` | `rec_edit` | Call record modified |

### Client Events
| Event Type | Timestamp Source | Description |
|------------|------------------|-------------|
| `client_created` | `rec_add` | Client record created |
| `client_updated` | `rec_edit` | Client record modified |

*`client_onboarded` event not available due to schema drift.

### Communication Events
| Event Type | Timestamp Source | Description |
|------------|------------------|-------------|
| `communication_created` | `rec_add` | Communication record created |
| `communication_logged` | `communication_date` | When communication occurred (business event) |
| `communication_updated` | `rec_edit` | Communication record modified |

### Email Events
| Event Type | Timestamp Source | Description |
|------------|------------------|-------------|
| `email_created` | `rec_add` | Email record created |
| `email_sent` | `sent_date` | When email was sent (business event) |
| `email_updated` | `rec_edit` | Email record modified |

---

## Validation Queries

### 1. Event Count by Type and Source

```sql
SELECT
    event_type,
    event_timestamp_source,
    count() as event_count,
    min(timestamp) as earliest,
    max(timestamp) as latest
FROM org_123.timeline
GROUP BY event_type, event_timestamp_source
ORDER BY event_type;
```

### 2. Full Timeline (Recent)

```sql
SELECT
    timestamp,
    event_type,
    event_timestamp_source,
    entity_id,
    description
FROM org_123.timeline
ORDER BY timestamp DESC
LIMIT 50;
```

### 3. Source Distribution

```sql
SELECT
    source_table,
    count() as cnt,
    count(DISTINCT entity_id) as unique_entities
FROM org_123.timeline
GROUP BY source_table
ORDER BY cnt DESC;
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

### 5. Entity Lifecycle

```sql
SELECT
    entity_id,
    groupArray(event_type) as events,
    groupArray(event_timestamp_source) as sources,
    groupArray(timestamp) as timestamps
FROM org_123.timeline
WHERE entity_type = 'account'
GROUP BY entity_id
LIMIT 10;
```

---

## Materialized Views

### List All MVs

```sql
SELECT name, engine
FROM system.tables
WHERE database = 'org_123'
  AND engine = 'MaterializedView';
```

### Expected MVs

- `mv_redtail_account_to_timeline`
- `mv_redtail_activity_to_timeline`
- `mv_redtail_call_to_timeline`
- `mv_redtail_client_to_timeline`
- `mv_redtail_communication_to_timeline`
- `mv_redtail_email_to_timeline`

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
DROP VIEW IF EXISTS org_123.mv_redtail_email_to_timeline;

-- Drop timeline table
DROP TABLE IF EXISTS org_123.timeline;

-- Optionally drop database
DROP DATABASE IF EXISTS org_123;
```

Then re-run: `airflow dags trigger silver__timeline_org_123`

---

## Known Issues

### Schema Drift (redtail_silver.client)

The actual `redtail_silver.client` table is missing columns defined in schema.yaml:
- `risk_tolerance` - removed from MV metadata
- `client_since` - removed entire `client_onboarded` event

**Resolution:** Either update the silver table schema or keep the MV simplified.

---

## Support

**Questions?** Contact Data Engineering team.
