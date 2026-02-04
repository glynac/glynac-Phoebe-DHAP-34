-- ============================================================
-- Materialized View: Redtail Communication → Timeline
-- ============================================================
-- Organization: 29a436a3-b5de-4afd-9c7a-059246c5a681
-- Source: redtail_silver.communication
-- Target: org_123.timeline
--
-- Events generated per record (backfill strategy):
--   1. communication_created (rec_add) - always
--   2. communication_logged (communication_date) - business event, when exists and != rec_add
--   3. communication_updated (rec_edit) - when exists and != rec_add
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS org_123.mv_redtail_communication_to_timeline
TO org_123.timeline
AS
-- Event 1: communication_created (from rec_add)
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'communication_created' AS event_type,
    rec_add AS timestamp,
    'rec_add' AS event_timestamp_source,
    'communication' AS entity_type,
    concat('communication_', toString(rec_id)) AS entity_id,
    concat('Communication created: ', COALESCE(communication_type, 'Communication'), ' - ', COALESCE(subject, 'No subject')) AS description,
    'redtail' AS source_system,
    'redtail_silver.communication' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'communication_type', COALESCE(communication_type, ''),
        'direction', COALESCE(direction, ''),
        'comm_status', COALESCE(comm_status, ''),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'from_user_id', toString(COALESCE(from_user_id, 0)),
        'backfill', 'true'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.communication
WHERE rec_id IS NOT NULL
  AND rec_add IS NOT NULL
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681'

UNION ALL

-- Event 2: communication_logged (from communication_date) - business event
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'communication_logged' AS event_type,
    communication_date AS timestamp,
    'communication_date' AS event_timestamp_source,
    'communication' AS entity_type,
    concat('communication_', toString(rec_id)) AS entity_id,
    concat(COALESCE(communication_type, 'Communication'), ' (', COALESCE(direction, 'Unknown'), '): ', COALESCE(subject, 'No subject')) AS description,
    'redtail' AS source_system,
    'redtail_silver.communication' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'communication_type', COALESCE(communication_type, ''),
        'direction', COALESCE(direction, ''),
        'comm_status', COALESCE(comm_status, ''),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'from_user_id', toString(COALESCE(from_user_id, 0)),
        'backfill', 'true'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.communication
WHERE rec_id IS NOT NULL
  AND communication_date IS NOT NULL
  AND (rec_add IS NULL OR communication_date != rec_add)
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681'

UNION ALL

-- Event 3: communication_updated (from rec_edit) - when different from rec_add
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'communication_updated' AS event_type,
    rec_edit AS timestamp,
    'rec_edit' AS event_timestamp_source,
    'communication' AS entity_type,
    concat('communication_', toString(rec_id)) AS entity_id,
    concat('Communication updated: ', COALESCE(communication_type, 'Communication'), ' (current state)') AS description,
    'redtail' AS source_system,
    'redtail_silver.communication' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'communication_type', COALESCE(communication_type, ''),
        'direction', COALESCE(direction, ''),
        'comm_status', COALESCE(comm_status, ''),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'from_user_id', toString(COALESCE(from_user_id, 0)),
        'backfill', 'true',
        'represents', 'final_state'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.communication
WHERE rec_id IS NOT NULL
  AND rec_edit IS NOT NULL
  AND rec_add IS NOT NULL
  AND rec_edit != rec_add
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681';
