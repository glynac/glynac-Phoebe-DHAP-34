-- ============================================================
-- Materialized View: Redtail Contact → Timeline
-- ============================================================
-- Organization: 29a436a3-b5de-4afd-9c7a-059246c5a681
-- Source: redtail_silver.contact
-- Target: org_123.timeline
--
-- Events generated per record (backfill strategy):
--   1. contact_created (rec_add) - always
--   2. contact_updated (rec_edit) - when exists and != rec_add
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS org_123.mv_redtail_contact_to_timeline
TO org_123.timeline
AS
-- Event 1: contact_created [timestamp: rec_add]
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'contact_created' AS event_type,
    rec_add AS timestamp,
    'rec_add' AS event_timestamp_source,
    'contact' AS entity_type,
    concat('contact_', toString(rec_id)) AS entity_id,
    concat('Contact ''', COALESCE(full_name_hash, 'Unknown'), ''' created - ', COALESCE(contact_type, 'Contact')) AS description,
    'redtail' AS source_system,
    'redtail_silver.contact' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'contact_type', COALESCE(contact_type, ''),
        'status', COALESCE(status_normalized, ''),
        'company', COALESCE(company, ''),
        'job_title', COALESCE(job_title, ''),
        'owner_id', toString(COALESCE(owner_id, 0)),
        'backfill', 'true'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.contact
WHERE rec_id IS NOT NULL
  AND rec_add IS NOT NULL
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681'

UNION ALL

-- Event 2: contact_updated [timestamp: rec_edit] - when rec_edit != rec_add
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'contact_updated' AS event_type,
    rec_edit AS timestamp,
    'rec_edit' AS event_timestamp_source,
    'contact' AS entity_type,
    concat('contact_', toString(rec_id)) AS entity_id,
    concat('Contact ''', COALESCE(full_name_hash, 'Unknown'), ''' information updated') AS description,
    'redtail' AS source_system,
    'redtail_silver.contact' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'contact_type', COALESCE(contact_type, ''),
        'status', COALESCE(status_normalized, ''),
        'company', COALESCE(company, ''),
        'job_title', COALESCE(job_title, ''),
        'backfill', 'true',
        'represents', 'final_state'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.contact
WHERE rec_id IS NOT NULL
  AND rec_edit IS NOT NULL
  AND rec_add IS NOT NULL
  AND rec_edit != rec_add
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681';
