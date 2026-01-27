# Organization 123 - Canonical Event Timeline

**Org ID:** 123
**Status:** ✅ Pilot - Active Testing
**Timeline Table:** `silver.org_123_timeline`
**Last Updated:** January 27, 2026

---

## Overview

This is the **pilot organization** for testing the Canonical Event Timeline implementation. Currently configured with 3 Redtail tables:
- ✅ Contact events
- ✅ Call events
- ✅ Email events

---

## Structure

```
org_123/
├── README.md                  ← You are here
├── base_timeline/             ← Timeline table
│   ├── dag.yaml              → DAG: silver__org_123_timeline
│   └── table.sql             → CREATE TABLE silver.org_123_timeline
├── mv_redtail_contact/        ← Contact MV
│   ├── dag.yaml              → DAG: silver__org_123_mv_redtail_contact_to_timeline
│   └── mv.sql                → CREATE MATERIALIZED VIEW (contact → timeline)
├── mv_redtail_call/           ← Call MV
│   ├── dag.yaml              → DAG: silver__org_123_mv_redtail_call_to_timeline
│   └── mv.sql                → CREATE MATERIALIZED VIEW (call → timeline)
└── mv_redtail_email/          ← Email MV
    ├── dag.yaml              → DAG: silver__org_123_mv_redtail_email_to_timeline
    └── mv.sql                → CREATE MATERIALIZED VIEW (email → timeline)
```

---

## Quick Start Testing

### Option 1: Direct SQL (Fastest - No Airflow needed)

```bash
# Navigate to project root
cd /Users/nurdin/Documents/Springer_Capital/repository-glynac/Airflow-prod

# Step 1: Create base timeline table
clickhouse-client -q "$(cat airflow-dag-configs/config/silver/timeline/org_123/base_timeline/table.sql)"

# Step 2: Create materialized views
clickhouse-client -q "$(cat airflow-dag-configs/config/silver/timeline/org_123/mv_redtail_contact/mv.sql)"
clickhouse-client -q "$(cat airflow-dag-configs/config/silver/timeline/org_123/mv_redtail_call/mv.sql)"
clickhouse-client -q "$(cat airflow-dag-configs/config/silver/timeline/org_123/mv_redtail_email/mv.sql)"

# Step 3: Verify
clickhouse-client -q "SHOW TABLES FROM silver LIKE '%timeline%' OR LIKE 'mv_%'"
clickhouse-client -q "SELECT count() FROM silver.org_123_timeline"
```

### Option 2: Via Airflow (Requires DAG generator update)

```bash
# Step 1: Sync configs to MinIO
cd airflow-dag-configs
mc mirror --overwrite config/ myminio/airflow-configs/config/

# Step 2: Restart Airflow or wait for DAG discovery

# Step 3: Trigger DAGs
airflow dags trigger silver__org_123_timeline
sleep 30  # Wait for table creation
airflow dags trigger silver__org_123_mv_redtail_contact_to_timeline
airflow dags trigger silver__org_123_mv_redtail_call_to_timeline
airflow dags trigger silver__org_123_mv_redtail_email_to_timeline

# Step 4: Monitor in Airflow UI
# http://localhost:8080/dags
```

---

## Validation Queries

### 1. Check Timeline Table Exists

```sql
SELECT
    database,
    name,
    engine,
    partition_key,
    sorting_key,
    primary_key,
    total_rows,
    formatReadableSize(total_bytes) as size
FROM system.tables
WHERE database = 'silver' AND name = 'org_123_timeline';
```

**Expected:** 1 row with ReplacingMergeTree engine

---

### 2. Check Materialized Views

```sql
SELECT
    name,
    engine,
    as_select
FROM system.tables
WHERE database = 'silver'
  AND name LIKE 'mv_redtail_%_to_timeline'
ORDER BY name;
```

**Expected:** 3 rows (contact, call, email MVs)

---

### 3. Event Count by Source

```sql
SELECT
    source_table,
    event_type,
    count() as event_count,
    min(timestamp) as earliest,
    max(timestamp) as latest
FROM silver.org_123_timeline
GROUP BY source_table, event_type
ORDER BY event_count DESC;
```

**Expected:**
```
┌─source_table─────────────┬─event_type──────┬─event_count─┬─earliest──────────┬─latest────────────┐
│ silver.redtail_contact   │ contact_created │     X,XXX   │ 2024-01-01...     │ 2026-01-27...     │
│ silver.redtail_call      │ call_made       │       XXX   │ 2024-01-01...     │ 2026-01-27...     │
│ silver.redtail_email     │ email_sent      │       XXX   │ 2024-01-01...     │ 2026-01-27...     │
└──────────────────────────┴─────────────────┴─────────────┴───────────────────┴───────────────────┘
```

---

### 4. Sample Timeline Events

```sql
SELECT
    timestamp,
    event_type,
    entity_type,
    entity_id,
    description
FROM silver.org_123_timeline
ORDER BY timestamp DESC
LIMIT 20;
```

**Expected:** Chronological list of events across all sources

---

### 5. Entity-Centric Query (Contact Timeline)

```sql
SELECT
    timestamp,
    event_type,
    description,
    source_system
FROM silver.org_123_timeline
WHERE org_id = 123
  AND entity_type = 'contact'
  AND entity_id = 'contact_12345'  -- Replace with real entity_id
ORDER BY timestamp DESC;
```

**Expected:** All events for a specific contact

---

### 6. Event Type Distribution

```sql
SELECT
    event_type,
    count() as count,
    count(DISTINCT entity_id) as unique_entities,
    formatReadableSize(sum(length(description))) as description_size
FROM silver.org_123_timeline
GROUP BY event_type
ORDER BY count DESC;
```

---

### 7. Data Quality Checks

```sql
-- Check for NULLs in required fields
SELECT
    'event_id' as field,
    countIf(event_id IS NULL) as null_count
FROM silver.org_123_timeline
UNION ALL
SELECT 'org_id', countIf(org_id IS NULL)
FROM silver.org_123_timeline
UNION ALL
SELECT 'event_type', countIf(event_type = '')
FROM silver.org_123_timeline
UNION ALL
SELECT 'timestamp', countIf(timestamp IS NULL)
FROM silver.org_123_timeline;
```

**Expected:** All null_count = 0

---

## Test Scenarios

### Scenario 1: Real-Time Event Creation

**Test:** New record in source table automatically creates timeline event

```sql
-- 1. Check current count
SELECT count() FROM silver.org_123_timeline WHERE source_table = 'silver.redtail_contact';

-- 2. Trigger redtail_contact Silver DAG to add new data
-- (Or wait for natural data updates)

-- 3. Verify new events appeared
SELECT count() FROM silver.org_123_timeline WHERE source_table = 'silver.redtail_contact';
-- Should be +N events
```

---

### Scenario 2: Cross-Source Timeline Query

**Test:** Query events from multiple sources for a date range

```sql
SELECT
    timestamp,
    source_system,
    event_type,
    description
FROM silver.org_123_timeline
WHERE org_id = 123
  AND timestamp >= '2026-01-01'
  AND timestamp < '2026-02-01'
ORDER BY timestamp DESC
LIMIT 100;
```

**Expected:** Events from all 3 sources (contact, call, email) interleaved chronologically

---

### Scenario 3: Performance Test

**Test:** Entity timeline query uses primary key efficiently

```sql
EXPLAIN PLAN
SELECT *
FROM silver.org_123_timeline
WHERE org_id = 123
  AND entity_type = 'contact'
  AND entity_id = 'contact_12345'
ORDER BY timestamp DESC;
```

**Expected:** Query plan shows primary key usage

---

## Troubleshooting

### Issue: Timeline table is empty

**Symptoms:**
```sql
SELECT count() FROM silver.org_123_timeline;
-- Returns: 0
```

**Possible causes:**
1. Redtail source tables are empty
2. Materialized views not created
3. Materialized views created but not backfilled

**Solution:**
```sql
-- Check 1: Verify source tables exist and have data
SELECT count() FROM silver.redtail_contact;
SELECT count() FROM silver.redtail_call;
SELECT count() FROM silver.redtail_email;

-- Check 2: Verify MVs exist
SHOW TABLES FROM silver LIKE 'mv_%';

-- Check 3: If MVs exist but timeline empty, drop and recreate MVs
DROP VIEW IF EXISTS silver.mv_redtail_contact_to_timeline;
-- Then recreate (ClickHouse will backfill automatically)
```

---

### Issue: Duplicate events

**Symptoms:**
```sql
SELECT event_id, count() as cnt
FROM silver.org_123_timeline
GROUP BY event_id
HAVING cnt > 1;
-- Returns rows (duplicates found)
```

**Solution:**
```sql
-- Force merge to deduplicate
OPTIMIZE TABLE silver.org_123_timeline FINAL;

-- Verify duplicates removed
SELECT event_id, count() as cnt
FROM silver.org_123_timeline
GROUP BY event_id
HAVING cnt > 1;
-- Should return 0 rows
```

---

### Issue: Events missing for some sources

**Symptoms:** Only contact events, no call/email events

**Solution:**
```sql
-- Check which MVs actually exist
SHOW TABLES FROM silver LIKE 'mv_%';

-- Check which source tables have data
SELECT 'contact' as source, count() as cnt FROM silver.redtail_contact
UNION ALL
SELECT 'call', count() FROM silver.redtail_call
UNION ALL
SELECT 'email', count() FROM silver.redtail_email;

-- If source table empty, trigger Silver DAG for that source
-- airflow dags trigger silver__redtail_call
-- airflow dags trigger silver__redtail_email
```

---

## Current Status

| Component | Status | Details |
|-----------|--------|---------|
| **Base Table** | ✅ Ready | DDL defined in `base_timeline/table.sql` |
| **Contact MV** | ✅ Ready | DDL defined in `mv_redtail_contact/mv.sql` |
| **Call MV** | ✅ Ready | DDL defined in `mv_redtail_call/mv.sql` |
| **Email MV** | ✅ Ready | DDL defined in `mv_redtail_email/mv.sql` |
| **Validation** | ✅ Ready | Queries defined above |
| **Testing** | ⏳ Pending | Waiting for execution |
| **Documentation** | ✅ Complete | You're reading it! |

---

## Next Steps

1. ✅ **Week 1 Complete**: 3 Redtail tables (contact, call, email)
2. ⏳ **Add 15 more Redtail tables**: account, household, portfolio, etc.
3. ⏳ **Week 2: Orion integration**: Add Orion tables to timeline
4. ⏳ **Week 2: Wealthbox integration**: Add Wealthbox tables to timeline
5. ⏳ **Week 2: Agent integration**: Update Portfolio Agent to query timeline

---

## Contact

**Owner:** Data Engineering Team
**Pilot Org:** Organization 123
**Testing Status:** Ready for validation

---

## Related Documentation

- [Timeline Main README](../README.md) - Multi-org overview
- [Timeline Schema](../../../../ALL-Docs/Silver/17_canonical_event_timeline_schema.md)
- [Event Taxonomy](../../../../ALL-Docs/Silver/18_event_type_taxonomy.md)
- [Redtail Mappings](../../../../ALL-Docs/Silver/19_redtail_event_mappings.md)
