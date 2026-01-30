-- ============================================================
-- Materialized View: Redtail Contact → Timeline
-- ============================================================
-- Organization: 29a436a3-b5de-4afd-9c7a-059246c5a681
-- Source: redtail_silver.contact
-- Target: redtail_silver.org_123_timeline
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS redtail_silver.mv_redtail_contact_to_timeline
TO redtail_silver.org_123_timeline
AS
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'contact_created' AS event_type,
    COALESCE(rec_add, processing_timestamp, now()) AS timestamp,
    'contact' AS entity_type,
    concat('contact_', toString(rec_id)) AS entity_id,
    concat(
        COALESCE(first_name, ''), ' ',
        COALESCE(last_name, ''), ' - ',
        COALESCE(contact_type, 'Contact'), ' created'
    ) AS description,
    'redtail' AS source_system,
    'redtail_silver.contact' AS source_table,
    toString(rec_id) AS source_id,
    NULL AS minio_path,
    toJSONString(map(
        'contact_type', COALESCE(contact_type, ''),
        'status', COALESCE(status, ''),
        'status_normalized', COALESCE(status_normalized, ''),
        'owner_id', toString(COALESCE(owner_id, 0)),
        'email', COALESCE(email, ''),
        'phone', COALESCE(phone, '')
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.contact
WHERE rec_id IS NOT NULL
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681';
