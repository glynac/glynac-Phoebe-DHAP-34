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

**Future Expansion (15+ Redtail Tables)**:
- `redtail_account` → `account_created`, `account_updated`
- `redtail_household` → `household_created`, `household_updated`
- `redtail_portfolio` → `portfolio_created`, `portfolio_rebalanced`
- `redtail_trade` → `trade_executed`, `trade_settled`
- `redtail_note` → `note_added`
- ... (12 more tables)

---

## Architecture Assessment

### Current Approach: Materialized Views from Silver Tables

**Architecture:**
```
Kafka → Flink → MinIO (Parquet) → Bronze (MV) → Silver Tables → Timeline MVs → Timeline Table
```

**How It Works:**
1. **Silver tables remain unchanged** (source of truth)
2. **Materialized Views** transform Silver → Timeline in real-time
3. **One unified timeline table** per org contains ALL events from ALL sources
4. **Automatic backfilling** when MV is created (ClickHouse feature)
5. **Low latency** (<1 second from Silver insert to Timeline)

### Why Materialized Views?

| Factor | Analysis |
|--------|----------|
| **Risk** | ✅ Low risk - No changes to existing Silver pipeline |
| **Decoupling** | ✅ Silver and Timeline evolve independently |
| **Backfilling** | ✅ Automatic - ClickHouse backfills existing Silver data when MV created |
| **Multiple Consumers** | ✅ Silver tables can be used by other systems simultaneously |
| **Real-Time** | ✅ <1 second latency - Acceptable for agent queries |
| **Team Independence** | ✅ Timeline team doesn't need Flink expertise |

### Alternative Approaches Considered

#### Option 1: Materialized Views from Silver (CURRENT - ✅ CHOSEN)

**Pros:**
- ✅ Zero risk to existing Kafka-Flink-Silver pipeline
- ✅ Silver tables remain source of truth
- ✅ Automatic backfilling of historical data
- ✅ Can add/remove timeline sources without touching Flink
- ✅ Timeline queries don't impact Silver performance
- ✅ Team can work independently

**Cons:**
- ⚠️ ~500ms-1s latency (Silver → Timeline) - ACCEPTABLE for agent queries
- ⚠️ Extra storage (timeline duplicates Silver data) - MANAGEABLE with TTL

**Best For:** Current phase with 1-50 orgs, agent query workloads, small team

---

#### Option 2: Dual-Write from Flink (Timeline as Secondary Target)

**Architecture:**
```
Kafka → Flink → [MinIO + Timeline Table] (dual write)
                     ↓
               Bronze → Silver
```

**Pros:**
- ✅ Real-time (~50-100ms from Kafka to Timeline)
- ✅ No duplicate storage (Timeline gets data directly)

**Cons:**
- ❌ HIGH RISK - Must modify Flink jobs (breaks if Flink changes)
- ❌ Tight coupling - Timeline changes require Flink redeployment
- ❌ Complex backfilling - Must replay Kafka or write custom jobs
- ❌ Flink expertise required - Blocks timeline team
- ❌ Single point of failure - Flink outage breaks timeline

**Best For:** Mature teams with strong Flink expertise, ultra-low latency requirements (<100ms)

---

#### Option 3: Timeline as Primary (Replace Silver)

**Architecture:**
```
Kafka → Flink → MinIO → Bronze → Timeline Table (PRIMARY)
                                      ↓
                                  Silver Views (MVs from Timeline)
```

**Pros:**
- ✅ Single source of truth (Timeline)
- ✅ No duplicate storage

**Cons:**
- ❌ EXTREME RISK - Complete rewrite of existing Silver pipeline
- ❌ Breaking change for all downstream consumers
- ❌ Must migrate 50+ existing Silver tables
- ❌ Rigid schema - Hard to add table-specific fields
- ❌ Performance risk - Timeline becomes bottleneck

**Best For:** Greenfield projects, not recommended for existing pipelines

---

### Performance Characteristics (Option 1 - Current)

**Latency:**
- Kafka → Silver: ~100-500ms (existing pipeline)
- Silver → Timeline: ~500ms-1s (MV processing)
- **Total Kafka → Timeline: ~600ms-1.5s** ✅ Acceptable for agent queries

**Storage:**
- Timeline table: ~30-40% of Silver storage (only core fields)
- TTL: 2 years (configurable)
- Compression: LZ4 (ClickHouse default)

**Query Performance:**
- Entity timeline query: <100ms (primary key optimized)
- Cross-source timeline query: <500ms (partition pruning)
- Full org timeline: <2s (with date filter)

---

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

---

## Why Organization-Based Structure?

### Benefits

| Benefit | Description |
|---------|-------------|
| **Multi-Tenancy** | Each org has separate timeline table for data isolation |
| **Scalability** | Can handle 100s of orgs without table explosion |
| **Independent Management** | Drop/backup/migrate org timelines independently |
| **Clear Ownership** | Easy to see which configs belong to which org |
| **Parallel Development** | Teams can work on different orgs simultaneously |

### Alternative Considered

**Unified Table Approach:**
```
silver.timeline (single table for all orgs, partitioned by org_id)
```

**Why NOT chosen for now:**
- Harder to drop single org data
- More complex ACL management
- Potential performance issues with 100s of orgs
- Can migrate to unified approach later if needed

---

## Current Organizations

| Org ID | Status | Timeline Table | MVs Count |
|--------|--------|----------------|-----------|
| **123** | ✅ Pilot - Active | `silver.org_123_timeline` | 3 (contact, call, email) |
| 456 | ⏳ Pending | `silver.org_456_timeline` | - |
| 789 | ⏳ Pending | `silver.org_789_timeline` | - |

---

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

**Quick test:**
```bash
cd /Users/nurdin/Documents/Springer_Capital/repository-glynac/Airflow-prod

# Option 1: Direct SQL (fastest)
clickhouse-client --multiquery < scripts/timeline/01_create_timeline_table.sql

# Option 2: Via Airflow (requires DAG generator update)
mc mirror --overwrite airflow-dag-configs/config/ myminio/airflow-configs/config/
airflow dags trigger silver__org_123_timeline
```

---

## Scaling Strategy

### Phase 1: Per-Org Tables (Current)
- Each org gets separate table: `org_123_timeline`, `org_456_timeline`
- Simple, isolated, easy to manage
- **Good for:** 1-50 organizations

### Phase 2: Unified Table (Future)
- Single table: `silver.timeline` (partitioned by org_id)
- Row-level security for multi-tenancy
- **Good for:** 50+ organizations
- **Migration:** Create unified table, keep per-org tables as MVs

### Phase 3: Distributed (Future)
- ClickHouse Distributed table across multiple shards
- Each shard handles subset of orgs
- **Good for:** 500+ organizations

---

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

## Migration Path

### From Per-Org to Unified

When org count > 50, migrate to unified table:

```sql
-- Step 1: Create unified table
CREATE TABLE silver.timeline AS silver.org_123_timeline
ENGINE = ReplacingMergeTree(_version)
PARTITION BY (org_id, toYYYYMM(processing_date))
ORDER BY (org_id, entity_type, entity_id, timestamp, event_type)
PRIMARY KEY (org_id, entity_type, entity_id);

-- Step 2: Migrate data from all per-org tables
INSERT INTO silver.timeline
SELECT * FROM silver.org_123_timeline;

INSERT INTO silver.timeline
SELECT * FROM silver.org_456_timeline;
-- ... etc

-- Step 3: Update MVs to point to unified table
-- (Recreate MVs with TO silver.timeline)

-- Step 4: Drop old per-org tables (after validation)
DROP TABLE silver.org_123_timeline;
```

---

## Monitoring

### Check All Org Timelines

```sql
-- List all timeline tables
SELECT
    database,
    name,
    engine,
    total_rows,
    formatReadableSize(total_bytes) as size
FROM system.tables
WHERE database = 'silver'
  AND name LIKE '%timeline'
ORDER BY name;
```

### Event Counts Per Org

```sql
-- Assuming unified table (future)
SELECT
    org_id,
    count() as total_events,
    count(DISTINCT event_type) as unique_event_types,
    count(DISTINCT entity_id) as unique_entities
FROM silver.timeline
GROUP BY org_id
ORDER BY org_id;
```

### Per-Org Health Check

```sql
-- Check org 123 timeline
SELECT
    'org_123' as org,
    count() as events,
    min(timestamp) as earliest_event,
    max(timestamp) as latest_event,
    count(DISTINCT source_table) as source_count
FROM silver.org_123_timeline;
```

---

## Related Documentation

- [Org 123 README](org_123/README.md) - Testing guide and validation queries for pilot org
- [Implementation Summary](IMPLEMENTATION_SUMMARY.md) - Complete implementation details and testing instructions

---

## Support

**Questions?** See [Org 123 README](org_123/README.md) for detailed testing and troubleshooting.
