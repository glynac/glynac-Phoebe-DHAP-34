-- ============================================================
-- Materialized View: Redtail Account → Timeline
-- ============================================================
-- Organization: 29a436a3-b5de-4afd-9c7a-059246c5a681
-- Source: redtail_silver.account
-- Target: org_123.timeline
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS org_123.mv_redtail_account_to_timeline
TO org_123.timeline
AS
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'account_created' AS event_type,
    COALESCE(open_date, rec_add, processing_timestamp, now()) AS timestamp,
    'account' AS entity_type,
    concat('account_', toString(rec_id)) AS entity_id,
    concat(
        'Account: ',
        COALESCE(account_name, 'Unnamed'), ' - ',
        COALESCE(account_type, 'Account'), ' (',
        COALESCE(account_status, 'Unknown'), ')'
    ) AS description,
    'redtail' AS source_system,
    'redtail_silver.account' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add,
    rec_edit,
    toJSONString(map(
        'account_type', COALESCE(account_type, ''),
        'account_status', COALESCE(account_status, ''),
        'status_normalized', COALESCE(status_normalized, ''),
        'institution_name', COALESCE(institution_name, ''),
        'institution_type', COALESCE(institution_type, ''),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'portfolio_id', toString(COALESCE(portfolio_id, 0)),
        'is_active', toString(COALESCE(is_active, 0))
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.account
WHERE rec_id IS NOT NULL
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681';
