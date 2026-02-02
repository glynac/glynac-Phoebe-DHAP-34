-- ============================================================
-- Materialized View: Redtail Email → Timeline
-- ============================================================
-- Organization: 29a436a3-b5de-4afd-9c7a-059246c5a681
-- Source: redtail_silver.email
-- Target: org_123.timeline
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS org_123.mv_redtail_email_to_timeline
TO org_123.timeline
AS
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'email_sent' AS event_type,
    COALESCE(
        parseDateTimeBestEffortOrNull(toString(sent_date)),
        parseDateTimeBestEffortOrNull(toString(rec_add)),
        parseDateTimeBestEffortOrNull(toString(processing_timestamp)),
        now()
    ) AS timestamp,
    'email' AS entity_type,
    concat('email_', toString(rec_id)) AS entity_id,
    concat(
        'Email: ',
        COALESCE(subject, 'No subject')
    ) AS description,
    'redtail' AS source_system,
    'redtail_silver.email' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    parseDateTimeBestEffortOrNull(toString(rec_add)) AS rec_add,
    parseDateTimeBestEffortOrNull(toString(rec_edit)) AS rec_edit,
    toJSONString(map(
        'email_id', toString(COALESCE(email_id, 0)),
        'email_status', COALESCE(email_status, ''),
        'from_address', COALESCE(from_address, ''),
        'to_addresses', COALESCE(to_addresses, ''),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'sent_by', toString(COALESCE(sent_by, 0)),
        'is_read', toString(COALESCE(is_read, false)),
        'has_attachments', toString(COALESCE(has_attachments, false)),
        'attachments_count', toString(COALESCE(attachments_count, 0))
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.email
WHERE rec_id IS NOT NULL
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681';
