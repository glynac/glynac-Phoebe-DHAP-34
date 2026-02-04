# Canonical Event Timeline

**Version:** 3.0
**Last Updated:** 2026-02-04

---

## Overview

The Canonical Event Timeline provides a **unified activity feed** that combines events from multiple source tables into a single, chronologically-ordered timeline per organization.

### Key Features

| Feature | Description |
|---------|-------------|
| **Per-org Database** | Dedicated database per org (e.g., `org_123`) for complete data isolation |
| **Multi-event per Record** | Each source record generates 2-3 events (created, business event, updated) |
| **Event Timestamp Source** | Tracks which field was used for event timestamp (`rec_add`, `open_date`, etc.) |
| **Real-time Capture** | MVs trigger on INSERT - no scheduled jobs needed after setup |
| **Automatic Backfill** | Existing data is backfilled during MV creation |

---

## Two-Phase Timeline Strategy

### Phase 1: Backfill (Historical Data)

When the DAG runs, it creates MVs and backfills existing data. Each source record generates **2-3 events**:

```
Source Record (account)
├── Event 1: account_created   [timestamp: rec_add]     - Always created
├── Event 2: account_opened    [timestamp: open_date]   - Business event (if exists and != rec_add)
└── Event 3: account_updated   [timestamp: rec_edit]    - If rec_edit != rec_add
```

### Phase 2: Ongoing (Real-time)

After backfill, MVs automatically capture new inserts. Each new record inserted into source tables triggers the MV to insert events into the timeline.

### Event Timestamp Source Column

The `event_timestamp_source` column tracks which field was used for the event's timestamp:

| event_type | event_timestamp_source | Description |
|------------|------------------------|-------------|
| `account_created` | `rec_add` | When record was created in source |
| `account_opened` | `open_date` | Business event - when account opened |
| `account_updated` | `rec_edit` | When record was last modified |
| `call_made` | `call_date` | Business event - when call occurred |
| `email_sent` | `sent_date` | Business event - when email was sent |

---

## Architecture

### Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SOURCE TABLES (Silver)                             │
├─────────────────────────────────────────────────────────────────────────────┤
│  redtail_silver.account      redtail_silver.activity                        │
│  redtail_silver.call         redtail_silver.client                          │
│  redtail_silver.communication  redtail_silver.email                         │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │
                                    │ INSERT triggers MV
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      MATERIALIZED VIEWS (per org)                           │
├─────────────────────────────────────────────────────────────────────────────┤
│  org_123.mv_redtail_account_to_timeline                                     │
│  org_123.mv_redtail_activity_to_timeline                                    │
│  org_123.mv_redtail_call_to_timeline                                        │
│  org_123.mv_redtail_client_to_timeline                                      │
│  org_123.mv_redtail_communication_to_timeline                               │
│  org_123.mv_redtail_email_to_timeline                                       │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │
                                    │ UNION ALL (2-3 events per record)
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          TIMELINE TABLE (unified)                           │
├─────────────────────────────────────────────────────────────────────────────┤
│  org_123.timeline                                                           │
│  - All events combined and chronologically ordered                          │
│  - Partitioned by processing_date                                           │
│  - ReplacingMergeTree for deduplication                                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

### DAG Structure

```
silver__timeline_org_123
│
├── base_timeline (TaskGroup)
│   ├── create_table      → Creates org_123 database + timeline table
│   └── validate_table    → Verifies table exists
│
├── mv_redtail_account (TaskGroup)
│   ├── create_mv         → Creates MV + backfills existing data
│   └── validate_mv       → Verifies events inserted (>= 1)
│
├── mv_redtail_activity (TaskGroup)
├── mv_redtail_call (TaskGroup)
├── mv_redtail_client (TaskGroup)
├── mv_redtail_communication (TaskGroup)
└── mv_redtail_email (TaskGroup)

Dependencies: All MV TaskGroups depend on base_timeline
```

---

## Directory Structure

```
config/silver/timeline/
├── README.md                              ← You are here (deployment guide)
└── org_123/                               ← Organization folder
    ├── README.md                          ← Org-specific documentation
    ├── dag.yaml                           ← DAG configuration
    ├── base_timeline/
    │   └── table.sql                      ← CREATE DATABASE + TABLE + INDEXES
    ├── mv_redtail_account/
    │   └── mv.sql                         ← MV with UNION ALL (3 events)
    ├── mv_redtail_activity/
    │   └── mv.sql
    ├── mv_redtail_call/
    │   └── mv.sql
    ├── mv_redtail_client/
    │   └── mv.sql                         ← Note: 2 events (client_since missing)
    ├── mv_redtail_communication/
    │   └── mv.sql
    └── mv_redtail_email/
        └── mv.sql
```

---

## Timeline Table Schema

```sql
CREATE TABLE org_123.timeline
(
    -- Core event fields
    event_id UUID,
    org_id String,
    event_type LowCardinality(String),
    timestamp DateTime64(3),
    event_timestamp_source LowCardinality(String),  -- NEW: tracks timestamp origin
    entity_type LowCardinality(String),
    entity_id String,
    description String,

    -- Source tracking
    source_system LowCardinality(String),
    source_table String,
    source_id String,
    minio_path Nullable(String),

    -- Audit timestamps from source
    rec_add Nullable(DateTime64(3)),
    rec_edit Nullable(DateTime64(3)),

    -- Flexible metadata (JSON)
    metadata Nullable(String),

    -- Partitioning & system
    processing_date Date,
    _loaded_at DateTime DEFAULT now(),
    _version Int32 DEFAULT 1
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(processing_date)
ORDER BY (org_id, entity_type, entity_id, timestamp, event_type)
```

---

## Current Event Types (org_123)

| Source Table | Event Types | Events per Record |
|--------------|-------------|-------------------|
| `redtail_silver.account` | `account_created`, `account_opened`, `account_updated` | 3 |
| `redtail_silver.activity` | `activity_created`, `activity_logged`, `activity_updated` | 3 |
| `redtail_silver.call` | `call_created`, `call_made`, `call_updated` | 3 |
| `redtail_silver.client` | `client_created`, `client_updated` | 2* |
| `redtail_silver.communication` | `communication_created`, `communication_logged`, `communication_updated` | 3 |
| `redtail_silver.email` | `email_created`, `email_sent`, `email_updated` | 3 |

*`client_onboarded` event skipped due to `client_since` column missing in actual table.

---

## Deploying to a New Organization

### Prerequisites

1. Source silver tables exist (e.g., `redtail_silver.account`, `redtail_silver.activity`, etc.)
2. Source tables have data for the target organization (`glynac_organization_id`)
3. Access to ClickHouse and MinIO

### Step 1: Copy org_123 Folder

```bash
# Create new org folder
cp -r config/silver/timeline/org_123 config/silver/timeline/org_456
```

### Step 2: Update Organization Details

**Files to update:**

| File | Changes Required |
|------|-----------------|
| `dag.yaml` | Update `dag_id`, `org_id`, `database`, `table_path` |
| `base_timeline/table.sql` | Replace `org_123` with `org_456` |
| `mv_redtail_*/mv.sql` | Replace `org_123` and org UUID |
| `README.md` | Update org-specific details |

**Search and replace:**
```bash
# In all files under org_456/
# Replace:
#   org_123 → org_456
#   29a436a3-b5de-4afd-9c7a-059246c5a681 → NEW_ORG_UUID
```

### Step 3: Verify Source Table Columns

**IMPORTANT:** Check that the actual silver tables have the columns referenced in MVs.

```sql
-- Check what columns exist in actual table
SELECT name FROM system.columns
WHERE database = 'redtail_silver' AND table = 'account';
```

If columns are missing (schema drift), remove them from the MV's metadata map and/or remove entire event types that depend on missing columns.

### Step 4: Sync to MinIO

```bash
mc mirror --overwrite config/ myminio/airflow-configs/
```

### Step 5: Trigger DAG

```bash
# Trigger the DAG
airflow dags trigger silver__timeline_org_456

# Monitor progress
airflow tasks list silver__timeline_org_456
```

### Step 6: Verify Deployment

```sql
-- Check timeline has events
SELECT
    event_type,
    event_timestamp_source,
    count() as cnt
FROM org_456.timeline
GROUP BY event_type, event_timestamp_source
ORDER BY event_type;

-- Check MVs exist
SELECT name FROM system.tables
WHERE database = 'org_456' AND engine = 'MaterializedView';
```

---

## MV SQL Template (UNION ALL Pattern)

Each MV generates multiple events per source record:

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS org_123.mv_redtail_account_to_timeline
TO org_123.timeline
AS
-- Event 1: account_created [timestamp: rec_add]
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'account_created' AS event_type,
    rec_add AS timestamp,
    'rec_add' AS event_timestamp_source,  -- Track timestamp source
    'account' AS entity_type,
    concat('account_', toString(rec_id)) AS entity_id,
    concat('Account created: ', COALESCE(account_name, 'Unnamed')) AS description,
    'redtail' AS source_system,
    'redtail_silver.account' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'account_type', COALESCE(account_type, ''),
        'backfill', 'true'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.account
WHERE rec_id IS NOT NULL
  AND rec_add IS NOT NULL
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681'

UNION ALL

-- Event 2: account_opened [timestamp: open_date] - business event
SELECT
    ...
    'account_opened' AS event_type,
    open_date AS timestamp,
    'open_date' AS event_timestamp_source,
    ...
WHERE open_date IS NOT NULL
  AND (rec_add IS NULL OR open_date != rec_add)  -- Avoid duplicate if same as rec_add

UNION ALL

-- Event 3: account_updated [timestamp: rec_edit]
SELECT
    ...
    'account_updated' AS event_type,
    rec_edit AS timestamp,
    'rec_edit' AS event_timestamp_source,
    ...
WHERE rec_edit IS NOT NULL
  AND rec_add IS NOT NULL
  AND rec_edit != rec_add;  -- Only if actually updated
```

---

## Troubleshooting

### Error: "Unknown expression identifier" (Column doesn't exist)

**Cause:** MV references a column that doesn't exist in the actual silver table (schema drift).

**Solution:**
1. Check actual table columns: `SELECT name FROM system.columns WHERE database = 'redtail_silver' AND table = 'TABLE_NAME';`
2. Remove missing column from MV's metadata map
3. If column is used for business event timestamp, remove that entire UNION ALL block

**Example fix for missing `risk_tolerance`:**
```sql
-- Remove this line from metadata map:
'risk_tolerance', COALESCE(risk_tolerance, ''),
```

### Error: "Timeline has 0 events"

**Possible causes:**
1. **Regex failed to extract SELECT:** Fixed in timeline_handler.py (handles comments between AS and SELECT)
2. **No data for org_id:** Check source table has data for the specific `glynac_organization_id`
3. **All records filtered out:** Check WHERE conditions (e.g., `rec_add IS NOT NULL`)

**Debug:**
```sql
-- Check source has data for this org
SELECT count() FROM redtail_silver.account
WHERE glynac_organization_id = 'YOUR_ORG_UUID';

-- Check if rec_add is NULL for all records
SELECT count(), countIf(rec_add IS NULL) as null_count
FROM redtail_silver.account
WHERE glynac_organization_id = 'YOUR_ORG_UUID';
```

### Cleanup and Re-deploy

```sql
-- Drop all MVs first (order doesn't matter)
DROP VIEW IF EXISTS org_123.mv_redtail_account_to_timeline;
DROP VIEW IF EXISTS org_123.mv_redtail_activity_to_timeline;
DROP VIEW IF EXISTS org_123.mv_redtail_call_to_timeline;
DROP VIEW IF EXISTS org_123.mv_redtail_client_to_timeline;
DROP VIEW IF EXISTS org_123.mv_redtail_communication_to_timeline;
DROP VIEW IF EXISTS org_123.mv_redtail_email_to_timeline;

-- Drop timeline table
DROP TABLE IF EXISTS org_123.timeline;

-- Optionally drop entire database
DROP DATABASE IF EXISTS org_123;
```

Then re-trigger the DAG.

---

## Handler Configuration

The timeline handler (`airflow-generated-dags/dags/utils/timeline_handler.py`) provides:

| Function | Description |
|----------|-------------|
| `create_timeline_table` | Creates database + table + indexes |
| `create_materialized_view` | Creates MV + runs backfill INSERT |
| `validate_timeline` | Validates data quality (non-null, event counts) |

### Key Handler Features

- **Backfill logic:** Extracts SELECT from MV DDL and runs `INSERT INTO timeline SELECT ...`
- **Regex for comment handling:** Handles SQL comments between `AS` and `SELECT`
- **Source verification:** Checks source table exists and has data before creating MV

---

## Query Examples

### Full Timeline (Recent)

```sql
SELECT
    timestamp,
    event_type,
    event_timestamp_source,
    description
FROM org_123.timeline
ORDER BY timestamp DESC
LIMIT 50;
```

### Events by Type and Timestamp Source

```sql
SELECT
    event_type,
    event_timestamp_source,
    count() as cnt,
    min(timestamp) as earliest,
    max(timestamp) as latest
FROM org_123.timeline
GROUP BY event_type, event_timestamp_source
ORDER BY event_type, event_timestamp_source;
```

### Track Record Lifecycle

```sql
SELECT
    entity_id,
    groupArray(event_type) as events,
    groupArray(timestamp) as timestamps,
    groupArray(event_timestamp_source) as sources
FROM org_123.timeline
WHERE entity_type = 'account'
GROUP BY entity_id
ORDER BY entity_id
LIMIT 10;
```

### Find Records with Updates

```sql
SELECT
    entity_id,
    countIf(event_type LIKE '%_created') as created_events,
    countIf(event_type LIKE '%_updated') as updated_events
FROM org_123.timeline
GROUP BY entity_id
HAVING updated_events > 0
ORDER BY updated_events DESC;
```

---

## Adding New Event Sources

### 1. Create MV Folder

```bash
mkdir config/silver/timeline/org_123/mv_redtail_note
```

### 2. Create mv.sql

Follow the UNION ALL pattern with 2-3 events per record. Always include:
- `event_timestamp_source` column
- `backfill: 'true'` in metadata
- Appropriate WHERE clauses to filter org_id

### 3. Update dag.yaml

Add new component to the `components` section.

### 4. Sync and Run

```bash
mc mirror --overwrite config/ myminio/airflow-configs/
airflow dags trigger silver__timeline_org_123
```

---

## Known Limitations

1. **Schema Drift:** If silver tables are missing columns defined in schema.yaml, MVs need manual adjustment
2. **One-time Backfill:** Backfill runs once during MV creation; historical data changes after creation won't be reflected
3. **No CDC:** Timeline captures INSERT events only, not UPDATE/DELETE on source records

---

## Support

**Questions?** Contact Data Engineering team.
