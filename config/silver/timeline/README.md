# Canonical Event Timeline

**Version:** 2.0

---

## Overview

The Canonical Event Timeline provides a **unified activity feed** that combines events from multiple source tables into a single, chronologically-ordered timeline per organization.

### Key Architecture

| Feature | Implementation |
|---------|----------------|
| **Database** | Dedicated database per org (e.g., `org_123`) |
| **Table** | Single `timeline` table per org |
| **MVs** | Materialized Views auto-insert from source tables |
| **Real-time** | MVs trigger on INSERT - no scheduled jobs needed |
| **Multi-tenant** | Complete data isolation per organization |

### DAG Pattern

**ONE DAG with TaskGroups** (like bronze layer pattern):

```
silver__timeline_org_123
├── base_timeline (TaskGroup)
│   ├── create_database
│   ├── create_table
│   └── validate_table
├── mv_redtail_account (TaskGroup)
│   ├── create_mv
│   ├── backfill_data
│   └── validate_mv
├── mv_redtail_activity (TaskGroup)
├── mv_redtail_call (TaskGroup)
├── mv_redtail_client (TaskGroup)
└── mv_redtail_communication (TaskGroup)
```

---

## How It Works

### Data Flow

```
1. SOURCE DATA                    2. MV TRIGGERS                 3. TIMELINE
   (Silver tables)                   (Automatic)                    (Unified)

┌─────────────────────┐          ┌──────────────────┐          ┌─────────────────┐
│ redtail_silver.     │          │ org_123.mv_      │          │ org_123.        │
│ account             │──INSERT──│ redtail_account  │──AUTO────│ timeline        │
│ activity            │          │ _to_timeline     │  INSERT  │                 │
│ call                │          │                  │          │ (all events     │
│ client              │          │                  │          │  combined)      │
│ communication       │          │                  │          │                 │
└─────────────────────┘          └──────────────────┘          └─────────────────┘
```

### One-Time Setup vs Daily Operations

```
┌─────────────────────────────────────────────────────────────────┐
│  Airflow DAG runs ONCE                                          │
│  ─────────────────────────────────────────────────              │
│  • Creates org_123 database                                     │
│  • Creates timeline table                                       │
│  • Creates all MVs                                              │
│  • Backfills existing data                                      │
│  • Done! DAG's job is finished.                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  ClickHouse handles EVERYTHING after that (forever)             │
│  ─────────────────────────────────────────────────              │
│                                                                 │
│  Day 1:  New accounts inserted → MV auto-inserts to timeline    │
│  Day 2:  New clients inserted  → MV auto-inserts to timeline    │
│  Day 3:  New communications    → MV auto-inserts to timeline    │
│  ...                                                            │
│  Forever: No Airflow needed - ClickHouse MVs work 24/7          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Schema Design

### Timeline Table Schema

```sql
CREATE TABLE org_123.timeline
(
    -- Core event fields
    event_id UUID,                              -- Unique event ID
    org_id String,                              -- UUID (e.g., '29a436a3-...')
    event_type LowCardinality(String),          -- 'account_created', 'call_made', etc.
    timestamp DateTime64(3),                    -- When event occurred
    entity_type LowCardinality(String),         -- 'account', 'call', 'client', etc.
    entity_id String,                           -- 'account_1', 'call_5', etc.
    description String,                         -- Human-readable description

    -- Source tracking
    source_system LowCardinality(String),       -- 'redtail'
    source_table String,                        -- 'redtail_silver.account'
    source_id String,                           -- Original rec_id
    minio_path Nullable(String),                -- For future document links

    -- Audit timestamps from source
    rec_add Nullable(DateTime64(3)),            -- When created in source system
    rec_edit Nullable(DateTime64(3)),           -- When last edited in source

    -- Flexible metadata
    metadata Nullable(String),                  -- JSON with source-specific fields

    -- Partitioning & system
    processing_date Date,
    _loaded_at DateTime DEFAULT now(),
    _version Int32 DEFAULT 1
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(processing_date)
ORDER BY (org_id, entity_type, entity_id, timestamp, event_type)
```

### Column Mapping (Unified Schema)

All source tables map to the same schema:

```
Source: account                    Source: communication
─────────────────                  ─────────────────────
rec_id ──────────────┐             rec_id ──────────────┐
account_name ────────┤             subject ─────────────┤
account_type ────────┤             communication_type ──┤
open_date ───────────┤             communication_date ──┤
rec_add ─────────────┤             rec_add ─────────────┤
rec_edit ────────────┤             rec_edit ────────────┤
                     │                                  │
                     ▼                                  ▼
              ┌──────────────────────────────────────────────┐
              │         org_123.timeline (unified)           │
              ├──────────────────────────────────────────────┤
              │  event_id      ← generateUUIDv4()            │
              │  org_id        ← glynac_organization_id      │
              │  event_type    ← 'account_created' / 'communication_logged' │
              │  timestamp     ← open_date / communication_date │
              │  entity_type   ← 'account' / 'communication' │
              │  entity_id     ← 'account_1' / 'communication_1' │
              │  description   ← transformed text            │
              │  rec_add       ← rec_add (from source)       │
              │  rec_edit      ← rec_edit (from source)      │
              │  metadata      ← JSON with source-specific fields │
              └──────────────────────────────────────────────┘
```

### Time Column Strategy: 4-Timestamp Pattern

| Column | Purpose | Source | Use Case |
|--------|---------|--------|----------|
| `timestamp` | Business event time | `open_date`, `call_date`, etc. | Timeline queries |
| `rec_add` | Source record creation | From source table | Audit trail |
| `rec_edit` | Source record edit | From source table | Change tracking |
| `_loaded_at` | Timeline ingestion | `now()` at insert | Technical debugging |

---

## Event Type Taxonomy

### Current Event Types (Week 1)

| Source Table | Event Type | Entity Type |
|--------------|------------|-------------|
| `redtail_silver.account` | `account_created` | `account` |
| `redtail_silver.activity` | `activity_logged` | `activity` |
| `redtail_silver.call` | `call_made` | `call` |
| `redtail_silver.client` | `client_created` | `client` |
| `redtail_silver.communication` | `communication_logged` | `communication` |

### Future Event Types

| Source Table | Event Type | Entity Type |
|--------------|------------|-------------|
| `redtail_silver.contact` | `contact_created` | `contact` |
| `redtail_silver.email` | `email_sent` | `email` |
| `redtail_silver.household` | `household_created` | `household` |
| `redtail_silver.note` | `note_added` | `note` |
| `redtail_silver.portfolio` | `portfolio_created` | `portfolio` |

### Naming Conventions

**Format**: `{entity}_{action}`

- Use lowercase with underscores (snake_case)
- Use past tense for completed actions (`created`, `logged`, not `create`, `log`)
- Keep entity name singular (`contact`, not `contacts`)

---

## Directory Structure

```
config/silver/timeline/
├── README.md                           ← You are here
└── org_123/                            ← Organization 123 (pilot)
    ├── README.md                       ← Org-specific guide
    ├── dag.yaml                        ← ONE DAG config (TaskGroups)
    ├── base_timeline/
    │   └── table.sql                   ← CREATE DATABASE + TABLE
    ├── mv_redtail_account/
    │   └── mv.sql                      ← MV: account → timeline
    ├── mv_redtail_activity/
    │   └── mv.sql                      ← MV: activity → timeline
    ├── mv_redtail_call/
    │   └── mv.sql                      ← MV: call → timeline
    ├── mv_redtail_client/
    │   └── mv.sql                      ← MV: client → timeline
    └── mv_redtail_communication/
        └── mv.sql                      ← MV: communication → timeline
```

---

## Adding a New Organization

### Step 1: Copy org_123 folder

```bash
cp -r config/silver/timeline/org_123 config/silver/timeline/org_456
```

### Step 2: Update all files

Replace in all files:
- `org_123` → `org_456`
- `29a436a3-b5de-4afd-9c7a-059246c5a681` → new org UUID

### Step 3: Sync to MinIO

```bash
mc mirror --overwrite config/ myminio/airflow-configs/config/
```

### Step 4: Run DAG

```bash
airflow dags trigger silver__timeline_org_456
```

---

## Query Examples

### Get full timeline

```sql
SELECT timestamp, event_type, description, rec_add, rec_edit
FROM org_123.timeline
ORDER BY timestamp DESC
LIMIT 50;
```

### Filter by event type

```sql
SELECT * FROM org_123.timeline
WHERE event_type = 'communication_logged'
ORDER BY timestamp DESC;
```

### Count events by type

```sql
SELECT
    event_type,
    count() as cnt,
    min(timestamp) as earliest,
    max(timestamp) as latest
FROM org_123.timeline
GROUP BY event_type
ORDER BY cnt DESC;
```

### Track record changes

```sql
SELECT
    entity_id,
    description,
    rec_add as created,
    rec_edit as last_modified,
    dateDiff('day', rec_add, rec_edit) as days_since_creation
FROM org_123.timeline
WHERE rec_edit IS NOT NULL
ORDER BY rec_edit DESC;
```

---

## Support

**Questions?** Contact Data Engineering team.
