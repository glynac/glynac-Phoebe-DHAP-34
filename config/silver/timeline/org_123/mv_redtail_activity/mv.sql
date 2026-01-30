-- ============================================================
-- Materialized View: Redtail Activity → Timeline
-- ============================================================
-- Organization: 29a436a3-b5de-4afd-9c7a-059246c5a681
-- Source: redtail_silver.activity
-- Target: redtail_silver.org_123_timeline
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS redtail_silver.mv_redtail_activity_to_timeline
TO redtail_silver.org_123_timeline
AS
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'activity_logged' AS event_type,
    COALESCE(activity_date, rec_add, processing_timestamp, now()) AS timestamp,
    'activity' AS entity_type,
    concat('activity_', toString(rec_id)) AS entity_id,
    concat(
        COALESCE(activity_type, 'Activity'), ': ',
        COALESCE(subject, 'No subject')
    ) AS description,
    'redtail' AS source_system,
    'redtail_silver.activity' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    toJSONString(map(
        'activity_type', COALESCE(activity_type, ''),
        'category', COALESCE(category, ''),
        'subcategory', COALESCE(subcategory, ''),
        'activity_status', COALESCE(activity_status, ''),
        'status_normalized', COALESCE(status_normalized, ''),
        'priority', COALESCE(priority, ''),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'owner_id', toString(COALESCE(owner_id, 0)),
        'duration_minutes', toString(COALESCE(duration_minutes, 0)),
        'follow_up_required', toString(COALESCE(follow_up_required, 0))
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.activity
WHERE rec_id IS NOT NULL
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681';
