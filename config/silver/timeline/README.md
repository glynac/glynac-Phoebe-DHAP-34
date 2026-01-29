# Canonical Event Timeline - Organization-Based Structure

**Version:** 1.0

---

## Overview

Timeline configurations are **organized by organization ID** for multi-tenancy and scalability. Each organization has its own:
- Timeline base table (`org_{id}_timeline`)
- Materialized views specific to that organization
- Independent data isolation

**DAG Generation:**
- Each `base_timeline/` creates DAG: `silver__org_{id}_base_timeline`
- Each `mv_*/` creates DAG: `silver__org_{id}_mv_{source}_to_timeline`
- All timeline DAGs tagged with: `['silver', 'timeline', 'org_{id}']`

---

## Task 1: Canonical Event Schema Design

### Schema Architecture

The timeline table uses **ReplacingMergeTree** engine for automatic deduplication:

```sql
ENGINE = ReplacingMergeTree(_version)
PARTITION BY (org_id, toYYYYMM(processing_date))
ORDER BY (org_id, entity_type, entity_id, timestamp, event_type)
PRIMARY KEY (org_id, entity_type, entity_id)
```

**Key Design Decisions:**

| Component | Design Choice | Rationale |
|-----------|---------------|-----------|
| **Table Engine** | ReplacingMergeTree(_version) | Automatic deduplication based on primary key + version |
| **Partitioning** | (org_id, toYYYYMM(processing_date)) | Multi-tenant isolation + time-based data lifecycle |
| **Primary Key** | (org_id, entity_type, entity_id) | Optimized for entity-centric queries |
| **Sorting Key** | (org_id, entity_type, entity_id, timestamp, event_type) | Chronological ordering within entity |

### Field Design

| Field | Type | Purpose | Example |
|-------|------|---------|---------|
| `event_id` | UUID | Unique event identifier | `550e8400-e29b-41d4-a716-446655440000` |
| `org_id` | Int32 | Organization identifier (multi-tenancy) | `123` |
| `event_type` | LowCardinality(String) | Event type from taxonomy | `contact_created` |
| `timestamp` | DateTime64(3) | **Business event time** (when event occurred) | `2026-01-27 14:30:00.123` |
| `entity_type` | LowCardinality(String) | Entity type | `contact`, `account`, `portfolio` |
| `entity_id` | String | Entity identifier | `contact_12345` |
| `description` | String | Human-readable event description | `John Doe created` |
| `source_system` | LowCardinality(String) | Source CRM system | `redtail`, `orion`, `wealthbox` |
| `source_table` | String | Source Silver table | `silver.redtail_contact` |
| `source_id` | String | Original record ID for lineage | `rec_id_12345` |
| `minio_path` | Nullable(String) | MinIO parquet path for evidence | `s3://bucket/path/file.parquet` |
| `metadata` | Nullable(String) | JSON metadata (flexible fields) | `{"category": "A", "status": "active"}` |
| `processing_date` | Date | **Processing date** (for partitioning) | `2026-01-27` |
| `_loaded_at` | DateTime | **Ingestion time** (when loaded to timeline) | `2026-01-27 14:31:00` |
| `_version` | Int32 | Version for deduplication | `1` |

### Time Column Strategy: 3-Timestamp Pattern

The schema uses **3 temporal columns** for different purposes:

1. **`timestamp`** (Business Event Time)
   - **Purpose**: When the business event actually occurred
   - **Source**: From source table (e.g., `rec_add`, `call_date`, `email_sent_at`)
   - **Use Case**: Chronological timeline queries, "Show me all events in January"
   - **Example**: `2026-01-15 09:30:00` (when customer called)

2. **`processing_date`** (Processing Date)
   - **Purpose**: Partitioning key for data lifecycle management
   - **Source**: When Flink/Airflow processed the record
   - **Use Case**: Data retention, partition pruning, cost optimization
   - **Example**: `2026-01-27` (when processed by pipeline)

3. **`_loaded_at`** (Ingestion Time)
   - **Purpose**: Technical audit trail
   - **Source**: `now()` when inserted into timeline
   - **Use Case**: Debugging, latency monitoring, data freshness checks
   - **Example**: `2026-01-27 14:31:00` (when written to timeline table)

**Why 3 timestamps?**
- Separates business semantics (`timestamp`) from technical operations (`processing_date`, `_loaded_at`)
- Enables late-arriving events without breaking partitioning
- Industry standard pattern (Snowflake, Databricks, dbt all use similar approach)

---

## Task 3: Event Type Taxonomy

### Event Categories (40+ Types)

The taxonomy organizes events into 5 major categories:

#### 1. Entity Lifecycle Events
Creation, modification, deletion of core entities.

**Event Types:**
- `contact_created`, `contact_updated`, `contact_deleted`
- `account_created`, `account_updated`, `account_closed`
- `household_created`, `household_updated`
- `portfolio_created`, `portfolio_rebalanced`
- `position_opened`, `position_closed`

#### 2. Communication Events
Interactions between advisors and clients.

**Event Types:**
- `call_made`, `call_received`, `call_missed`
- `email_sent`, `email_received`, `email_opened`
- `meeting_scheduled`, `meeting_completed`, `meeting_canceled`
- `note_added`, `task_created`, `task_completed`

#### 3. Financial Events
Transactions, trades, and financial activities.

**Event Types:**
- `trade_executed`, `trade_settled`, `trade_canceled`
- `dividend_received`, `interest_accrued`
- `fee_charged`, `commission_earned`
- `deposit_received`, `withdrawal_processed`

#### 4. Relationship Events
Changes in relationships between entities.

**Event Types:**
- `contact_linked_to_account`
- `account_linked_to_household`
- `advisor_assigned`, `advisor_changed`
- `beneficiary_added`, `beneficiary_removed`

#### 5. Workflow Events
Process and compliance activities.

**Event Types:**
- `document_uploaded`, `document_signed`
- `compliance_review_started`, `compliance_review_completed`
- `risk_assessment_updated`
- `onboarding_started`, `onboarding_completed`

### Naming Conventions

**Format**: `{entity}_{action}`

| Pattern | Example | Description |
|---------|---------|-------------|
| `{entity}_created` | `contact_created` | New entity created |
| `{entity}_updated` | `account_updated` | Entity modified |
| `{entity}_deleted` | `contact_deleted` | Entity soft/hard deleted |
| `{entity}_{action}` | `call_made` | Specific action performed |
| `{entity}_{status}` | `trade_settled` | Status change event |

**Best Practices:**
- Use lowercase with underscores (snake_case)
- Use past tense for completed actions (`created`, `updated`, not `create`, `update`)
- Keep entity name singular (`contact`, not `contacts`)
- Be specific for financial events (`trade_executed` vs generic `transaction_created`)

### Redtail Source Mappings (Week 1 - 3 Tables)

| Source Table | Event Type | Entity Type | Mapping Logic |
|--------------|------------|-------------|---------------|
| **redtail_contact** | `contact_created` | `contact` | `rec_add` → `timestamp` |
| **redtail_call** | `call_made` | `call` | `call_date` → `timestamp` |
| **redtail_email** | `email_sent` | `email` | `sent_date` → `timestamp` |

**Expansion (15+ Redtail Tables)**:
- `redtail_account` → `account_created`, `account_updated`
- `redtail_household` → `household_created`, `household_updated`
- `redtail_portfolio` → `portfolio_created`, `portfolio_rebalanced`
- `redtail_trade` → `trade_executed`, `trade_settled`
- `redtail_note` → `note_added`
- ... (12 more tables)


## Directory Structure

```
config/silver/timeline/
├── README.md                           ← You are here
├── org_123/                            ← Organization 123 (pilot)
│   ├── README.md                       ← Org-specific guide
│   ├── base_timeline/                  ← Base timeline table
│   │   ├── dag.yaml
│   │   └── table.sql
│   ├── mv_redtail_contact/             ← Contact → Timeline MV
│   │   ├── dag.yaml
│   │   └── mv.sql
│   ├── mv_redtail_call/                ← Call → Timeline MV
│   │   ├── dag.yaml
│   │   └── mv.sql
│   └── mv_redtail_email/               ← Email → Timeline MV
│       ├── dag.yaml
│       └── mv.sql
│
├── org_456/                            ← others: Organization 456
│   └── base_timeline/
│       ├── dag.yaml
│       └── table.sql
│
└── _template/                          ← Template for new orgs
    ├── base_timeline/
    │   ├── dag.yaml.template
    │   └── table.sql.template
    └── mv_redtail_contact/
        ├── dag.yaml.template
        └── mv.sql.template
```

## Adding a New Organization

### Quick Start

```bash
# Step 1: Copy template
cd config/silver/timeline
cp -r _template org_456

# Step 2: Update org_id in all files
find org_456 -type f -name "*.yaml" -exec sed -i '' 's/org_id: 123/org_id: 456/g' {} \;
find org_456 -type f -name "*.sql" -exec sed -i '' 's/org_123/org_456/g' {} \;
find org_456 -type f -name "*.sql" -exec sed -i '' 's/123 AS org_id/456 AS org_id/g' {} \;

# Step 3: Update DAG IDs
find org_456 -type f -name "dag.yaml" -exec sed -i '' 's/__org_123_/__org_456_/g' {} \;

# Step 4: Sync to MinIO
mc mirror --overwrite config/ myminio/airflow-configs/config/

# Step 5: Trigger DAGs in Airflow
airflow dags trigger silver__org_456_timeline
airflow dags trigger silver__org_456_mv_redtail_contact_to_timeline
# ... etc
```

### Detailed Steps

See **[Adding New Organization Guide](org_123/README.md#adding-new-organization)** for complete instructions.

---

## Testing Org 123 (Pilot)

See **[Org 123 README](org_123/README.md)** for detailed testing instructions.


## File Naming Conventions

### DAG IDs

```
silver__org_{org_id}_timeline                      # Base table
silver__org_{org_id}_mv_redtail_contact_to_timeline   # Contact MV
silver__org_{org_id}_mv_redtail_call_to_timeline      # Call MV
```

### Table Names

```
silver.org_{org_id}_timeline                       # Timeline table
silver.mv_{source}_{entity}_to_timeline_org_{id}   # MV name (alternative)
```

### Config Paths

```
config/silver/timeline/org_{org_id}/base_timeline/
config/silver/timeline/org_{org_id}/mv_{source}_{entity}/
```

---

## Support

**Questions?** Reach Data Engineer team.
