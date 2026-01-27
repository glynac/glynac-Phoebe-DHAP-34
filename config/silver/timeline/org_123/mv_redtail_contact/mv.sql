-- ============================================================
-- Materialized View: Redtail Contact → Timeline
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_redtail_contact_to_timeline
TO silver.org_123_timeline
AS
SELECT
    generateUUIDv4() AS event_id,
    123 AS org_id,
    'contact_created' AS event_type,
    COALESCE(rec_add, processing_timestamp, now()) AS timestamp,
    'contact' AS entity_type,
    concat('contact_', toString(rec_id)) AS entity_id,
    concat(first_name_hash, ' ', last_name_hash, ' created') AS description,
    'redtail' AS source_system,
    'silver.redtail_contact' AS source_table,
    toString(rec_id) AS source_id,
    NULL AS minio_path,
    toJSONString(map(
        'contact_type', contact_type,
        'status', status,
        'owner_id', toString(owner_id)
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM silver.redtail_contact;
