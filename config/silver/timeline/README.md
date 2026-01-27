# Canonical Event Timeline - Organization-Based Structure

**Version:** 1.0
**Date:** January 28, 2026
**Status:** ✅ Airflow Integration Complete - Ready for Testing

---

## 🎉 Implementation Complete!

**What's New:**
- ✅ **Airflow DAG Generator Integration**: Timeline configs now automatically generate DAGs
- ✅ **Timeline Handler Module**: Specialized functions for table/MV creation
- ✅ **3-Level Directory Support**: `timeline/{org_id}/{component}/` structure fully supported
- ✅ **Task Type Detection**: Automatic routing for `timeline_base_table` and `materialized_view` tasks

**Ready to Test:**
- Direct SQL execution (fastest)
- Full Airflow DAG execution
- See [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md) for complete details

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
├── org_456/                            ← Future: Organization 456
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

- [Org 123 README](org_123/README.md) - Testing guide for pilot org
- [Timeline Schema](../../../ALL-Docs/Silver/17_canonical_event_timeline_schema.md)
- [Event Taxonomy](../../../ALL-Docs/Silver/18_event_type_taxonomy.md)
- [Redtail Mappings](../../../ALL-Docs/Silver/19_redtail_event_mappings.md)
- [Implementation Scripts](../../../scripts/timeline/README.md)

---

## Support

**Questions?** See [Org 123 README](org_123/README.md) for detailed testing and troubleshooting.
