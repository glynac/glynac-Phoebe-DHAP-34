# Canonical Event Timeline - Implementation Summary

**Status:** ✅ Complete - Ready for Testing
**Date:** January 28, 2026
**Milestone:** Week 1 - Timeline Foundation

---

## Overview

The Canonical Event Timeline system has been fully implemented and integrated into the Airflow DAG generator. This provides a unified chronological view of all entity activities across multiple source systems (Redtail, Orion, Wealthbox).

---

## What Was Implemented

### 1. Documentation (ALL-Docs/Silver/)

**[17_canonical_event_timeline_schema.md](../../../ALL-Docs/Silver/17_canonical_event_timeline_schema.md)**
- Complete schema specification for timeline tables
- Field definitions and data types
- Performance considerations (partitioning, indexing)
- Usage patterns and query examples

**[18_event_type_taxonomy.md](../../../ALL-Docs/Silver/18_event_type_taxonomy.md)**
- 40+ event types across 5 categories:
  - Entity Lifecycle (created, updated, deleted)
  - Communication (call, email, meeting, message)
  - Financial (transaction, portfolio_updated, allocation_changed)
  - Relationship (association_created, household_linked)
  - Workflow (task_created, status_changed, note_added)
- Naming convention: `{entity}_{action}`
- Event type to system mapping

**[19_redtail_event_mappings.md](../../../ALL-Docs/Silver/19_redtail_event_mappings.md)**
- Detailed SQL transformations for all 18 Redtail tables
- Field mappings from source to canonical schema
- Metadata extraction strategies
- Example queries for each event type

---

### 2. Timeline Configs (airflow-dag-configs/config/silver/timeline/)

**Directory Structure:**
```
timeline/
├── README.md                     ← Main documentation
├── IMPLEMENTATION_SUMMARY.md     ← This file
└── org_123/                      ← Pilot organization
    ├── README.md                 ← Org-specific testing guide
    ├── base_timeline/            ← Timeline base table
    │   ├── dag.yaml             → DAG config (task_type: timeline_base_table)
    │   └── table.sql            → CREATE TABLE DDL
    ├── mv_redtail_contact/       ← Contact MV
    │   ├── dag.yaml             → DAG config (task_type: materialized_view)
    │   └── mv.sql               → CREATE MATERIALIZED VIEW
    ├── mv_redtail_call/          ← Call MV
    │   ├── dag.yaml
    │   └── mv.sql
    └── mv_redtail_email/         ← Email MV
        ├── dag.yaml
        └── mv.sql
```

**Key Features:**
- **Organization-based structure**: Per-org isolation for multi-tenancy
- **Pilot with 3 tables**: Contact, Call, Email (testing before full rollout)
- **Scalable architecture**: Clear path from per-org tables (1-50 orgs) to unified table (50+ orgs)

---

### 3. Timeline Handler Module (airflow-generated-dags/dags/utils/)

**[timeline_handler.py](../../../airflow-generated-dags/dags/utils/timeline_handler.py)**

**Functions:**
- `create_timeline_table(**context)`: Creates base timeline table from table.sql
- `create_materialized_view(**context)`: Creates MV from mv.sql with backfill
- `validate_timeline(**context)`: Validates timeline data (row counts, NULL checks, event distribution)

**Features:**
- Loads SQL from MinIO configs
- Connects to ClickHouse
- Executes DDL statements
- Verifies table/MV creation
- Provides detailed logging

---

### 4. DAG Generator Integration (airflow-generated-dags/dags/generated/)

**[silver_layer_dag_generator.py](../../../airflow-generated-dags/dags/generated/silver_layer_dag_generator.py)**

**Changes Made:**

1. **Timeline Handler Import (Lines 100-110)**
   ```python
   try:
       from timeline_handler import (
           create_timeline_table,
           create_materialized_view,
           validate_timeline
       )
       USING_TIMELINE_HANDLER = True
       logger.info("✅ Timeline handler imported successfully")
   except ImportError as e:
       logger.warning(f"⚠️  Timeline handler not available: {e}")
       USING_TIMELINE_HANDLER = False
   ```

2. **3-Level Directory Discovery (Lines 483-550)**
   - Updated `discover_silver_tables()` to handle timeline's 3-level nesting
   - Special case for `timeline/` folder: `timeline/{org_id}/{component}/`
   - Returns paths like: `['timeline/org_123/base_timeline', 'timeline/org_123/mv_redtail_contact']`

3. **Task Type Detection (Lines 1867-1930)**
   - Detects `task_type: timeline_base_table` → creates timeline table
   - Detects `task_type: materialized_view` → creates MV
   - Skips standard Silver transformation logic for timeline configs

4. **Timeline DAG Creation:**
   - **Base Table DAG**: `create_timeline_table` → `validate_timeline`
   - **MV DAG**: `create_materialized_view` (one task)

---

## How It Works

### Timeline Base Table Creation

1. **DAG Discovery**:
   - Generator scans `silver/timeline/org_123/base_timeline/`
   - Finds `dag.yaml` with `task_type: timeline_base_table`
   - Creates DAG: `silver__org_123_timeline`

2. **Task Execution**:
   ```
   create_timeline_table
   └→ Load table.sql from MinIO
   └→ Connect to ClickHouse
   └→ Execute CREATE TABLE statements
   └→ Verify table exists

   validate_timeline
   └→ Check row counts
   └→ Validate event types
   └→ Check for NULL values
   ```

3. **Result**:
   - Table created: `silver.org_123_timeline`
   - Engine: `ReplacingMergeTree(_version)`
   - Partition: `(org_id, toYYYYMM(processing_date))`
   - Primary key: `(org_id, entity_type, entity_id)`

### Materialized View Creation

1. **DAG Discovery**:
   - Generator scans `silver/timeline/org_123/mv_redtail_contact/`
   - Finds `dag.yaml` with `task_type: materialized_view`
   - Creates DAG: `silver__org_123_mv_redtail_contact_to_timeline`

2. **Task Execution**:
   ```
   create_materialized_view
   └→ Load mv.sql from MinIO
   └→ Check target table exists (silver.org_123_timeline)
   └→ Execute CREATE MATERIALIZED VIEW
   └→ ClickHouse automatically backfills from source
   └→ Verify MV exists
   ```

3. **Result**:
   - MV created: `silver.mv_redtail_contact_to_timeline`
   - Transforms: `silver.redtail_contact` → `silver.org_123_timeline`
   - Real-time: New contact records automatically create timeline events

---

## Testing Instructions

### Option 1: Direct SQL Execution (Fastest)

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

### Option 2: Via Airflow (Full Integration)

```bash
# Step 1: Sync configs to MinIO
cd airflow-dag-configs
mc mirror --overwrite config/ myminio/airflow-configs/config/

# Step 2: Restart Airflow scheduler (to pick up new DAG generator code)
# In your Airflow deployment, restart the scheduler pod/container

# Step 3: Wait for DAG discovery (check Airflow UI)
# http://localhost:8080/dags

# Step 4: Trigger DAGs in order
# First: Base table
airflow dags trigger silver__org_123_base_timeline

# Wait 30 seconds for table creation

# Then: Materialized views (can run in parallel)
airflow dags trigger silver__org_123_mv_redtail_contact_to_timeline
airflow dags trigger silver__org_123_mv_redtail_call_to_timeline
airflow dags trigger silver__org_123_mv_redtail_email_to_timeline

# Step 5: Monitor in Airflow UI
# Check task logs for success/errors
```

---

## Validation Queries

See [org_123/README.md](org_123/README.md) for comprehensive validation queries including:
1. Check timeline table exists
2. Check materialized views
3. Event count by source
4. Sample timeline events
5. Entity-centric queries
6. Event type distribution
7. Data quality checks

---

## Next Steps

### Week 1 Completion (Current)
- ✅ Timeline schema designed
- ✅ Event type taxonomy defined
- ✅ Redtail mappings documented
- ✅ Airflow integration complete
- ✅ 3 test MVs created (contact, call, email)
- ⏳ **Testing pending** (awaiting user execution)

### Week 1 Expansion
- ⏳ Add remaining 15 Redtail tables
- ⏳ Create MVs for: account, household, portfolio, transaction, note, etc.

### Week 2 - Multi-Source Integration
- ⏳ Add Orion event mappings
- ⏳ Add Wealthbox event mappings
- ⏳ Update Portfolio Agent to query timeline

### Future Enhancements
- ⏳ Unified timeline table (50+ orgs)
- ⏳ Real-time event streaming
- ⏳ Event aggregation views
- ⏳ Timeline API endpoints

---

## Troubleshooting

### DAGs Not Appearing in Airflow UI

**Possible Causes:**
1. Configs not synced to MinIO
2. Airflow scheduler not restarted
3. DAG generator errors

**Solution:**
```bash
# Check MinIO sync
mc ls myminio/airflow-configs/config/silver/timeline/

# Check Airflow scheduler logs
kubectl logs -f <scheduler-pod> | grep timeline
# or
docker logs -f <scheduler-container> | grep timeline

# Look for DAG generation errors
grep "timeline" /opt/airflow/logs/scheduler/latest/*.log
```

### Timeline Table Empty After MV Creation

**Possible Causes:**
1. Source tables (redtail_contact, etc.) are empty
2. MVs created but not backfilled
3. Organization ID mismatch

**Solution:**
```sql
-- Check source tables
SELECT count() FROM silver.redtail_contact;
SELECT count() FROM silver.redtail_call;
SELECT count() FROM silver.redtail_email;

-- Check MVs exist
SHOW TABLES FROM silver LIKE 'mv_%';

-- If MVs exist but timeline empty, drop and recreate
DROP VIEW IF EXISTS silver.mv_redtail_contact_to_timeline;
-- Then recreate using mv.sql (ClickHouse will backfill)
```

---

## Architecture Decisions

### Per-Org Tables (Current: 1-50 orgs)
- **Benefits**: Tenant isolation, easy management, clear boundaries
- **Trade-offs**: Multiple tables, schema changes require iteration
- **When to migrate**: 50+ organizations

### Unified Table (Future: 50+ orgs)
- **Benefits**: Single schema, easier queries, better analytics
- **Trade-offs**: Larger table, more complex partitioning
- **Migration path**: Documented in main README

---

## File Checklist

✅ **Documentation**
- [x] 17_canonical_event_timeline_schema.md
- [x] 18_event_type_taxonomy.md
- [x] 19_redtail_event_mappings.md

✅ **Configs**
- [x] timeline/README.md
- [x] timeline/IMPLEMENTATION_SUMMARY.md
- [x] timeline/org_123/README.md
- [x] timeline/org_123/base_timeline/dag.yaml
- [x] timeline/org_123/base_timeline/table.sql
- [x] timeline/org_123/mv_redtail_contact/dag.yaml
- [x] timeline/org_123/mv_redtail_contact/mv.sql
- [x] timeline/org_123/mv_redtail_call/dag.yaml
- [x] timeline/org_123/mv_redtail_call/mv.sql
- [x] timeline/org_123/mv_redtail_email/dag.yaml
- [x] timeline/org_123/mv_redtail_email/mv.sql

✅ **Code**
- [x] airflow-generated-dags/dags/utils/timeline_handler.py
- [x] airflow-generated-dags/dags/generated/silver_layer_dag_generator.py (updated)

---

## Contact

**Owner:** Data Engineering Team
**Pilot Org:** Organization 123
**Implementation Status:** Complete - Ready for Testing

**Next Action:** User to execute testing (Option 1 or Option 2 above)

---

## Related Documentation

- [Timeline Main README](README.md) - Multi-org overview and scaling strategy
- [Org 123 README](org_123/README.md) - Testing guide and validation queries
- [Event Schema](../../../ALL-Docs/Silver/17_canonical_event_timeline_schema.md) - Complete schema specification
- [Event Taxonomy](../../../ALL-Docs/Silver/18_event_type_taxonomy.md) - Event types and naming
- [Redtail Mappings](../../../ALL-Docs/Silver/19_redtail_event_mappings.md) - Source-to-timeline mappings
