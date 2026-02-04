-- ============================================================
-- Materialized View: Redtail Account → Timeline
-- ============================================================
-- Organization: 29a436a3-b5de-4afd-9c7a-059246c5a681
-- Source: redtail_silver.account
-- Target: org_123.timeline
--
-- Events generated per record (backfill strategy):
--   1. account_created (rec_add) - always
--   2. account_opened (open_date) - when exists and != rec_add
--   3. account_updated (rec_edit) - when exists and != rec_add
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS org_123.mv_redtail_account_to_timeline
TO org_123.timeline
AS
-- Event 1: account_created (from rec_add)
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'account_created' AS event_type,
    rec_add AS timestamp,
    'rec_add' AS event_timestamp_source,
    'account' AS entity_type,
    concat('account_', toString(rec_id)) AS entity_id,
    concat('Account created: ', COALESCE(account_name, 'Unnamed')) AS description,
    'redtail' AS source_system,
    'redtail_silver.account' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'account_type', COALESCE(account_type, ''),
        'account_status', COALESCE(account_status, ''),
        'institution_name', COALESCE(institution_name, ''),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'backfill', 'true'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.account
WHERE rec_id IS NOT NULL
  AND rec_add IS NOT NULL
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681'

UNION ALL

-- Event 2: account_opened (from open_date) - business event
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'account_opened' AS event_type,
    open_date AS timestamp,
    'open_date' AS event_timestamp_source,
    'account' AS entity_type,
    concat('account_', toString(rec_id)) AS entity_id,
    concat('Account opened: ', COALESCE(account_name, 'Unnamed'), ' - ', COALESCE(account_type, 'Account')) AS description,
    'redtail' AS source_system,
    'redtail_silver.account' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'account_type', COALESCE(account_type, ''),
        'account_status', COALESCE(account_status, ''),
        'institution_name', COALESCE(institution_name, ''),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'backfill', 'true'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.account
WHERE rec_id IS NOT NULL
  AND open_date IS NOT NULL
  AND (rec_add IS NULL OR open_date != rec_add)
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681'

UNION ALL

-- Event 3: account_updated (from rec_edit) - when different from rec_add
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'account_updated' AS event_type,
    rec_edit AS timestamp,
    'rec_edit' AS event_timestamp_source,
    'account' AS entity_type,
    concat('account_', toString(rec_id)) AS entity_id,
    concat('Account updated: ', COALESCE(account_name, 'Unnamed'), ' (current state)') AS description,
    'redtail' AS source_system,
    'redtail_silver.account' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'account_type', COALESCE(account_type, ''),
        'account_status', COALESCE(account_status, ''),
        'institution_name', COALESCE(institution_name, ''),
        'contact_id', toString(COALESCE(contact_id, 0)),
        'backfill', 'true',
        'represents', 'final_state'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.account
WHERE rec_id IS NOT NULL
  AND rec_edit IS NOT NULL
  AND rec_add IS NOT NULL
  AND rec_edit != rec_add
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681';
