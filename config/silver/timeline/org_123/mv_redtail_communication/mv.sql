-- ============================================================
-- Materialized View: Redtail Communication → Timeline
-- ============================================================
-- Organization: 29a436a3-b5de-4afd-9c7a-059246c5a681
-- Source: redtail_silver.communication
-- Target: org_123.timeline
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS org_123.mv_redtail_communication_to_timeline
TO org_123.timeline
AS
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'communication_logged' AS event_type,
    COALESCE(communication_date, rec_add, processing_timestamp, now()) AS timestamp,
    'communication' AS entity_type,
    concat('communication_', toString(rec_id)) AS entity_id,
    concat(
        COALESCE(communication_type, 'Communication'), ' (',
        COALESCE(direction, 'Unknown'), '): ',
        COALESCE(subject, 'No subject')
    ) AS description,
    'redtail' AS source_system,
    'redtail_silver.communication' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add,
    rec_edit,
    toJSONString(map(
        'communication_type', COALESCE(communication_type, ''),
        'direction', COALESCE(direction, ''),
        'comm_status', COALESCE(comm_status, ''),
        'status_normalized', COALESCE(status_normalized, ''),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'from_user_id', toString(COALESCE(from_user_id, 0)),
        'to_contact_id', toString(COALESCE(to_contact_id, 0)),
        'read_status', toString(COALESCE(read_status, false)),
        'attachments_count', toString(COALESCE(attachments_count, 0))
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.communication
WHERE rec_id IS NOT NULL
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681';
