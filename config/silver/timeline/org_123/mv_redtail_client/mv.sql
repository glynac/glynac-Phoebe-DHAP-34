-- ============================================================
-- Materialized View: Redtail Client → Timeline
-- ============================================================
-- Organization: 29a436a3-b5de-4afd-9c7a-059246c5a681
-- Source: redtail_silver.client
-- Target: org_123.timeline
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS org_123.mv_redtail_client_to_timeline
TO org_123.timeline
AS
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'client_created' AS event_type,
    COALESCE(onboarding_date, rec_add, processing_timestamp, now()) AS timestamp,
    'client' AS entity_type,
    concat('client_', toString(rec_id)) AS entity_id,
    concat(
        COALESCE(client_type, 'Client'), ' - ',
        COALESCE(service_level, 'Standard'), ' (',
        COALESCE(client_status, 'Unknown'), ')'
    ) AS description,
    'redtail' AS source_system,
    'redtail_silver.client' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add,
    rec_edit,
    toJSONString(map(
        'client_type', COALESCE(client_type, ''),
        'client_status', COALESCE(client_status, ''),
        'status_normalized', COALESCE(status_normalized, ''),
        'service_level', COALESCE(service_level, ''),
        'risk_profile', COALESCE(risk_profile, ''),
        'client_id', toString(COALESCE(client_id, 0)),
        'contact_id', toString(COALESCE(contact_id, 0))
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.client
WHERE rec_id IS NOT NULL
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681';
