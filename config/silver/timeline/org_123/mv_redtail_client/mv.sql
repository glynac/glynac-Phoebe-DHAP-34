-- ============================================================
-- Materialized View: Redtail Client → Timeline
-- ============================================================
-- Organization: 29a436a3-b5de-4afd-9c7a-059246c5a681
-- Source: redtail_silver.client
-- Target: org_123.timeline
--
-- Events generated per record (backfill strategy):
--   1. client_created (rec_add) - always
--   2. client_updated (rec_edit) - when exists and != rec_add
-- Note: client_since column doesn't exist in actual table, so client_onboarded event is skipped
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS org_123.mv_redtail_client_to_timeline
TO org_123.timeline
AS
-- Event 1: client_created [timestamp: rec_add]
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'client_created' AS event_type,
    rec_add AS timestamp,
    'rec_add' AS event_timestamp_source,
    'client' AS entity_type,
    concat('client_', toString(rec_id)) AS entity_id,
    concat('Client created: ', COALESCE(service_level, 'Standard'), ' - ', COALESCE(client_status, 'Unknown')) AS description,
    'redtail' AS source_system,
    'redtail_silver.client' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'client_status', COALESCE(client_status, ''),
        'service_level', COALESCE(service_level, ''),
        'client_id', toString(COALESCE(client_id, 0)),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'backfill', 'true'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.client
WHERE rec_id IS NOT NULL
  AND rec_add IS NOT NULL
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681'

UNION ALL

-- Event 2: client_updated [timestamp: rec_edit] - when rec_edit != rec_add
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'client_updated' AS event_type,
    rec_edit AS timestamp,
    'rec_edit' AS event_timestamp_source,
    'client' AS entity_type,
    concat('client_', toString(rec_id)) AS entity_id,
    concat('Client updated: ', COALESCE(service_level, 'Standard'), ' (current state)') AS description,
    'redtail' AS source_system,
    'redtail_silver.client' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'client_status', COALESCE(client_status, ''),
        'service_level', COALESCE(service_level, ''),
        'client_id', toString(COALESCE(client_id, 0)),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'backfill', 'true',
        'represents', 'final_state'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.client
WHERE rec_id IS NOT NULL
  AND rec_edit IS NOT NULL
  AND rec_add IS NOT NULL
  AND rec_edit != rec_add
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681';
