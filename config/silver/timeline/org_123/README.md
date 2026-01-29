# Organization 123 - Canonical Event Timeline

**Org ID:** 123
**Status:** ✅ Pilot - Active Testing
**Timeline Table:** `silver.org_123_timeline`

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

## Validation Queries

### . Event Count by Source

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
