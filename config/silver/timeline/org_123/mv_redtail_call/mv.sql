-- ============================================================
-- Materialized View: Redtail Call → Timeline
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS redtail_silver.mv_redtail_call_to_timeline
TO redtail_silver.org_123_timeline
AS
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'call_made' AS event_type,
    COALESCE(call_date, rec_add, processing_timestamp, now()) AS timestamp,
    'call' AS entity_type,
    concat('call_', toString(rec_id)) AS entity_id,
    concat(
        COALESCE(call_type, 'Call'), ' - ',
        COALESCE(subject, COALESCE(notes, 'Call made'))
    ) AS description,
    'redtail' AS source_system,
    'redtail_silver.call' AS source_table,
    toString(rec_id) AS source_id,
    NULL AS minio_path,
    toJSONString(map(
        'call_type', COALESCE(call_type, ''),
        'call_status', COALESCE(call_status, ''),
        'phone_number', COALESCE(phone_number, ''),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'user_id', toString(COALESCE(user_id, 0)),
        'duration_minutes', toString(COALESCE(duration_minutes, 0)),
        'disposition', COALESCE(disposition, ''),
        'outcome', COALESCE(outcome, '')
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.call
WHERE rec_id IS NOT NULL;
