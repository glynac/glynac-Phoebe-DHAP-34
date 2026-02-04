-- ============================================================
-- Materialized View: Redtail Activity → Timeline
-- ============================================================
-- Organization: 29a436a3-b5de-4afd-9c7a-059246c5a681
-- Source: redtail_silver.activity
-- Target: org_123.timeline
--
-- Events generated per record (backfill strategy):
--   1. activity_created (rec_add) - always
--   2. activity_logged (activity_date) - business event, when exists and != rec_add
--   3. activity_updated (rec_edit) - when exists and != rec_add
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS org_123.mv_redtail_activity_to_timeline
TO org_123.timeline
AS
-- Event 1: activity_created [timestamp: rec_add]
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'activity_created' AS event_type,
    rec_add AS timestamp,
    'rec_add' AS event_timestamp_source,
    'activity' AS entity_type,
    concat('activity_', toString(rec_id)) AS entity_id,
    concat('Activity created: ', COALESCE(activity_type, 'Activity'), ' - ', COALESCE(subject, 'No subject')) AS description,
    'redtail' AS source_system,
    'redtail_silver.activity' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'activity_type', COALESCE(activity_type, ''),
        'category', COALESCE(category, ''),
        'subcategory', COALESCE(subcategory, ''),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'owner_id', toString(COALESCE(owner_id, 0)),
        'activity_id', toString(COALESCE(activity_id, 0)),
        'backfill', 'true'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.activity
WHERE rec_id IS NOT NULL
  AND rec_add IS NOT NULL
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681'

UNION ALL

-- Event 2: activity_logged [timestamp: activity_date] - business event
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'activity_logged' AS event_type,
    activity_date AS timestamp,
    'activity_date' AS event_timestamp_source,
    'activity' AS entity_type,
    concat('activity_', toString(rec_id)) AS entity_id,
    concat(COALESCE(activity_type, 'Activity'), ': ', COALESCE(subject, 'No subject')) AS description,
    'redtail' AS source_system,
    'redtail_silver.activity' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'activity_type', COALESCE(activity_type, ''),
        'category', COALESCE(category, ''),
        'subcategory', COALESCE(subcategory, ''),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'owner_id', toString(COALESCE(owner_id, 0)),
        'activity_id', toString(COALESCE(activity_id, 0)),
        'backfill', 'true'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.activity
WHERE rec_id IS NOT NULL
  AND activity_date IS NOT NULL
  AND (rec_add IS NULL OR activity_date != rec_add)
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681'

UNION ALL

-- Event 3: activity_updated [timestamp: rec_edit] - when rec_edit != rec_add
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'activity_updated' AS event_type,
    rec_edit AS timestamp,
    'rec_edit' AS event_timestamp_source,
    'activity' AS entity_type,
    concat('activity_', toString(rec_id)) AS entity_id,
    concat('Activity updated: ', COALESCE(activity_type, 'Activity'), ' (current state)') AS description,
    'redtail' AS source_system,
    'redtail_silver.activity' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'activity_type', COALESCE(activity_type, ''),
        'category', COALESCE(category, ''),
        'subcategory', COALESCE(subcategory, ''),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'owner_id', toString(COALESCE(owner_id, 0)),
        'activity_id', toString(COALESCE(activity_id, 0)),
        'backfill', 'true',
        'represents', 'final_state'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.activity
WHERE rec_id IS NOT NULL
  AND rec_edit IS NOT NULL
  AND rec_add IS NOT NULL
  AND rec_edit != rec_add
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681';
