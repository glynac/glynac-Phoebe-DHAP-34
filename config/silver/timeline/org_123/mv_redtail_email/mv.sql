-- ============================================================
-- Materialized View: Redtail Email → Timeline
-- ============================================================
-- Organization: 29a436a3-b5de-4afd-9c7a-059246c5a681
-- Source: redtail_silver.email
-- Target: redtail_silver.org_123_timeline
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS redtail_silver.mv_redtail_email_to_timeline
TO redtail_silver.org_123_timeline
AS
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'email_sent' AS event_type,
    COALESCE(sent_date, rec_add, processing_timestamp, now()) AS timestamp,
    'email' AS entity_type,
    concat('email_', toString(rec_id)) AS entity_id,
    concat(
        'Email: ',
        COALESCE(subject, 'No subject')
    ) AS description,
    'redtail' AS source_system,
    'redtail_silver.email' AS source_table,
    toString(rec_id) AS source_id,
    NULL AS minio_path,
    toJSONString(map(
        'email_status', COALESCE(email_status, ''),
        'status_normalized', COALESCE(status_normalized, ''),
        'from_address', COALESCE(from_address, ''),
        'to_addresses', COALESCE(to_addresses, ''),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'sent_by', toString(COALESCE(sent_by, 0)),
        'is_read', toString(COALESCE(is_read, false))
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.email
WHERE rec_id IS NOT NULL
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681';
