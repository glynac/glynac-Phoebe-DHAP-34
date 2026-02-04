-- ============================================================
-- Materialized View: Redtail Email → Timeline
-- ============================================================
-- Organization: 29a436a3-b5de-4afd-9c7a-059246c5a681
-- Source: redtail_silver.email
-- Target: org_123.timeline
--
-- Events generated per record (backfill strategy):
--   1. email_created (rec_add) - always
--   2. email_sent (sent_date) - business event, when exists and != rec_add
--   3. email_updated (rec_edit) - when exists and != rec_add
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS org_123.mv_redtail_email_to_timeline
TO org_123.timeline
AS
-- Event 1: email_created [timestamp: rec_add]
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'email_created' AS event_type,
    rec_add AS timestamp,
    'rec_add' AS event_timestamp_source,
    'email' AS entity_type,
    concat('email_', toString(rec_id)) AS entity_id,
    concat('Email created: ', COALESCE(subject, 'No subject')) AS description,
    'redtail' AS source_system,
    'redtail_silver.email' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'email_id', toString(COALESCE(email_id, 0)),
        'email_status', COALESCE(email_status, ''),
        'from_address', COALESCE(from_address, ''),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'sent_by', toString(COALESCE(sent_by, 0)),
        'backfill', 'true'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.email
WHERE rec_id IS NOT NULL
  AND rec_add IS NOT NULL
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681'

UNION ALL

-- Event 2: email_sent [timestamp: sent_date] - business event
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'email_sent' AS event_type,
    sent_date AS timestamp,
    'sent_date' AS event_timestamp_source,
    'email' AS entity_type,
    concat('email_', toString(rec_id)) AS entity_id,
    concat('Email sent: ', COALESCE(subject, 'No subject')) AS description,
    'redtail' AS source_system,
    'redtail_silver.email' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'email_id', toString(COALESCE(email_id, 0)),
        'email_status', COALESCE(email_status, ''),
        'from_address', COALESCE(from_address, ''),
        'to_addresses', COALESCE(to_addresses, ''),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'sent_by', toString(COALESCE(sent_by, 0)),
        'backfill', 'true'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.email
WHERE rec_id IS NOT NULL
  AND sent_date IS NOT NULL
  AND (rec_add IS NULL OR sent_date != rec_add)
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681'

UNION ALL

-- Event 3: email_updated [timestamp: rec_edit] - when rec_edit != rec_add
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'email_updated' AS event_type,
    rec_edit AS timestamp,
    'rec_edit' AS event_timestamp_source,
    'email' AS entity_type,
    concat('email_', toString(rec_id)) AS entity_id,
    concat('Email updated: ', COALESCE(subject, 'No subject'), ' (current state)') AS description,
    'redtail' AS source_system,
    'redtail_silver.email' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'email_id', toString(COALESCE(email_id, 0)),
        'email_status', COALESCE(email_status, ''),
        'from_address', COALESCE(from_address, ''),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'sent_by', toString(COALESCE(sent_by, 0)),
        'backfill', 'true',
        'represents', 'final_state'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.email
WHERE rec_id IS NOT NULL
  AND rec_edit IS NOT NULL
  AND rec_add IS NOT NULL
  AND rec_edit != rec_add
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681';
