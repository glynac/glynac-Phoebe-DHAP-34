-- ============================================================
-- Materialized View: Redtail Call → Timeline
-- ============================================================
-- Organization: 29a436a3-b5de-4afd-9c7a-059246c5a681
-- Source: redtail_silver.call
-- Target: org_123.timeline
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS org_123.mv_redtail_call_to_timeline
TO org_123.timeline
AS
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'call_made' AS event_type,
    COALESCE(
        parseDateTimeBestEffortOrNull(toString(call_date)),
        parseDateTimeBestEffortOrNull(toString(rec_add)),
        parseDateTimeBestEffortOrNull(toString(processing_timestamp)),
        now()
    ) AS timestamp,
    'call' AS entity_type,
    concat('call_', toString(rec_id)) AS entity_id,
    concat(
        'Call: ',
        COALESCE(subject, 'No subject')
    ) AS description,
    'redtail' AS source_system,
    'redtail_silver.call' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    parseDateTimeBestEffortOrNull(toString(rec_add)) AS rec_add,
    parseDateTimeBestEffortOrNull(toString(rec_edit)) AS rec_edit,
    toJSONString(map(
        'call_type', COALESCE(call_type, ''),
        'call_status', COALESCE(call_status, ''),
        'duration_minutes', toString(COALESCE(duration_minutes, 0)),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'subject', COALESCE(subject, ''),
        'notes', COALESCE(notes, '')
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.call
WHERE rec_id IS NOT NULL
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681';
