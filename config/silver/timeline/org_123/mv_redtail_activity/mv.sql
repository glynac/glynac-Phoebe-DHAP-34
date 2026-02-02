-- ============================================================
-- Materialized View: Redtail Activity → Timeline
-- ============================================================
-- Organization: 29a436a3-b5de-4afd-9c7a-059246c5a681
-- Source: redtail_silver.activity
-- Target: org_123.timeline
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS org_123.mv_redtail_activity_to_timeline
TO org_123.timeline
AS
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'activity_logged' AS event_type,
    COALESCE(
        parseDateTimeBestEffortOrNull(toString(activity_date)),
        parseDateTimeBestEffortOrNull(toString(rec_add)),
        parseDateTimeBestEffortOrNull(toString(processing_timestamp)),
        now()
    ) AS timestamp,
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
    parseDateTimeBestEffortOrNull(toString(rec_add)) AS rec_add,
    parseDateTimeBestEffortOrNull(toString(rec_edit)) AS rec_edit,
    toJSONString(map(
        'activity_type', COALESCE(activity_type, ''),
        'category', COALESCE(category, ''),
        'subcategory', COALESCE(subcategory, ''),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'owner_id', toString(COALESCE(owner_id, 0)),
        'activity_id', toString(COALESCE(activity_id, 0))
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.activity
WHERE rec_id IS NOT NULL
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681';
