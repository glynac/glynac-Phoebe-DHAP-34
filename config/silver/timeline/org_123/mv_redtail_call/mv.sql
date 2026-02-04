-- ============================================================
-- Materialized View: Redtail Call → Timeline
-- ============================================================
-- Organization: 29a436a3-b5de-4afd-9c7a-059246c5a681
-- Source: redtail_silver.call
-- Target: org_123.timeline
--
-- Events generated per record (backfill strategy):
--   1. call_created (rec_add) - always
--   2. call_made (call_date) - business event, when exists and != rec_add
--   3. call_updated (rec_edit) - when exists and != rec_add
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS org_123.mv_redtail_call_to_timeline
TO org_123.timeline
AS
-- Event 1: call_created [timestamp: rec_add]
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'call_created' AS event_type,
    rec_add AS timestamp,
    'rec_add' AS event_timestamp_source,
    'call' AS entity_type,
    concat('call_', toString(rec_id)) AS entity_id,
    concat('Call record created: ', COALESCE(subject, 'No subject')) AS description,
    'redtail' AS source_system,
    'redtail_silver.call' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'call_type', COALESCE(call_type, ''),
        'call_status', COALESCE(call_status, ''),
        'duration_minutes', toString(COALESCE(duration_minutes, 0)),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'backfill', 'true'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.call
WHERE rec_id IS NOT NULL
  AND rec_add IS NOT NULL
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681'

UNION ALL

-- Event 2: call_made [timestamp: call_date] - business event
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'call_made' AS event_type,
    call_date AS timestamp,
    'call_date' AS event_timestamp_source,
    'call' AS entity_type,
    concat('call_', toString(rec_id)) AS entity_id,
    concat('Call: ', COALESCE(subject, 'No subject'), ' (', COALESCE(call_type, 'Unknown'), ')') AS description,
    'redtail' AS source_system,
    'redtail_silver.call' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'call_type', COALESCE(call_type, ''),
        'call_status', COALESCE(call_status, ''),
        'duration_minutes', toString(COALESCE(duration_minutes, 0)),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'backfill', 'true'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.call
WHERE rec_id IS NOT NULL
  AND call_date IS NOT NULL
  AND (rec_add IS NULL OR call_date != rec_add)
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681'

UNION ALL

-- Event 3: call_updated [timestamp: rec_edit] - when rec_edit != rec_add
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'call_updated' AS event_type,
    rec_edit AS timestamp,
    'rec_edit' AS event_timestamp_source,
    'call' AS entity_type,
    concat('call_', toString(rec_id)) AS entity_id,
    concat('Call updated: ', COALESCE(subject, 'No subject'), ' (current state)') AS description,
    'redtail' AS source_system,
    'redtail_silver.call' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'call_type', COALESCE(call_type, ''),
        'call_status', COALESCE(call_status, ''),
        'duration_minutes', toString(COALESCE(duration_minutes, 0)),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'backfill', 'true',
        'represents', 'final_state'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.call
WHERE rec_id IS NOT NULL
  AND rec_edit IS NOT NULL
  AND rec_add IS NOT NULL
  AND rec_edit != rec_add
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681';
