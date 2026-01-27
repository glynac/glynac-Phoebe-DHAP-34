-- ============================================================
-- Materialized View: Redtail Call → Timeline
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS silver.mv_redtail_call_to_timeline
TO silver.org_123_timeline
AS
SELECT
    generateUUIDv4() AS event_id,
    123 AS org_id,
    'call_made' AS event_type,
    COALESCE(rec_add, processing_timestamp, now()) AS timestamp,
    'call' AS entity_type,
    concat('call_', toString(rec_id)) AS entity_id,
    'Call made' AS description,
    'redtail' AS source_system,
    'silver.redtail_call' AS source_table,
    toString(rec_id) AS source_id,
    NULL AS minio_path,
    toJSONString(map()) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM silver.redtail_call;
