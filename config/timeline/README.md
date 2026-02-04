# Canonical Event Timeline Configuration

This directory contains configuration for the Canonical Event Timeline system.

## Directory Structure

```
config/timeline/
├── README.md                 # This file
└── org_{id}/                 # Per-organization timeline config
    ├── dag.yaml              # DAG configuration
    ├── base_timeline/
    │   └── table.sql         # Timeline table DDL
    └── sources/              # YAML configs for each source table
        ├── redtail_account.yaml
        ├── redtail_activity.yaml
        ├── redtail_call.yaml
        ├── redtail_client.yaml
        ├── redtail_communication.yaml
        ├── redtail_contact.yaml
        ├── redtail_email.yaml
        └── redtail_transaction.yaml
```

## How It Works

### 1. DAG Generation

The `timeline_dag_generator.py` discovers all `org_*` folders in this directory and creates a DAG for each organization.

**DAG Structure:**
- **One DAG per organization**: `timeline__org_{id}`
- **TaskGroups**:
  - `base_timeline`: Creates the timeline table
  - `{source_name}`: One TaskGroup per source (creates MV + validates)

### 2. YAML-Driven MV Generation

Instead of handwritten SQL files, MV SQL is auto-generated from YAML configs.

**Example source YAML:**
```yaml
version: "1.0"
source:
  database: redtail_silver
  table: account
  primary_key: rec_id
  org_filter_column: glynac_organization_id

timeline:
  entity_type: account
  mv_name: mv_redtail_account_to_timeline

  events:
    - type: account_created
      timestamp_field: rec_add
      timestamp_source: rec_add
      condition: "rec_add IS NOT NULL"
      description_template: "Account '{account_name}' created"
      metadata:
        account_type: "COALESCE(account_type, '')"
        account_number: "COALESCE(account_number, '')"
```

### 3. MV Auto-Update Magic

ClickHouse Materialized Views automatically capture new data:

1. **MV Creation**: When the MV is created, it defines the transformation
2. **Backfill**: Existing data is backfilled via manual INSERT
3. **Auto-Capture**: New inserts to the source table automatically flow to the timeline

No scheduled jobs needed for ongoing data - it's real-time!

## Phase 1 Events (15 Core Events)

| Source | Events |
|--------|--------|
| account | account_created, account_opened, account_updated |
| activity | activity_created, activity_scheduled, activity_completed, activity_updated |
| call | call_created, call_made, call_updated |
| client | client_created, client_updated |
| communication | communication_created, communication_sent, communication_updated |
| contact | contact_created, contact_updated |
| email | email_created, email_sent, email_updated |
| transaction | transaction_created, transaction_executed, transaction_settled, transaction_updated |

## Adding a New Organization

1. Create folder: `config/timeline/org_{new_id}/`
2. Copy `dag.yaml` from existing org and update:
   - `dag.dag_id`
   - `organization.org_id` (UUID)
   - `organization.database`
3. Copy `base_timeline/table.sql` and update database references
4. Copy `sources/` folder and update org references

## Adding a New Source

1. Create YAML in `sources/{source_name}.yaml`
2. Define:
   - `source`: database, table, primary_key, org_filter_column
   - `timeline.entity_type`: What entity this source represents
   - `timeline.mv_name`: Name for the materialized view
   - `timeline.events`: List of events to extract
3. Add source to `dag.yaml` under `sources`
4. Re-trigger the DAG

## Event Definition Reference

Each event in a source YAML has:

| Field | Required | Description |
|-------|----------|-------------|
| `type` | Yes | Event type name (e.g., `account_created`) |
| `timestamp_field` | Yes | Source column for event timestamp |
| `timestamp_source` | Yes | Human-readable source name (stored in `event_timestamp_source`) |
| `condition` | Yes | SQL WHERE condition for when event applies |
| `description_template` | Yes | Template with `{field}` placeholders |
| `metadata` | No | Dict of key: SQL expression for metadata JSON |

## Cleanup & Re-trigger

To reset the timeline for an org:

```sql
-- Drop all MVs
DROP VIEW IF EXISTS org_123.mv_redtail_account_to_timeline;
DROP VIEW IF EXISTS org_123.mv_redtail_activity_to_timeline;
-- ... repeat for all sources

-- Truncate timeline table (keep structure)
TRUNCATE TABLE org_123.timeline;
```

Then re-trigger the DAG in Airflow.

## Files Reference

| File | Purpose |
|------|---------|
| `timeline_dag_generator.py` | Generates Airflow DAGs from this config |
| `timeline_handler.py` | Creates tables, MVs, runs backfill, validates |
| `dag.yaml` | DAG settings, sources list, validation config |
| `base_timeline/table.sql` | Timeline table DDL (ClickHouse ReplacingMergeTree) |
| `sources/*.yaml` | Event definitions for each source table |
