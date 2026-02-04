-- ============================================================
-- Materialized View: Redtail Transaction → Timeline
-- ============================================================
-- Organization: 29a436a3-b5de-4afd-9c7a-059246c5a681
-- Source: redtail_silver.transaction
-- Target: org_123.timeline
--
-- Events generated per record (backfill strategy):
--   1. transaction_created (rec_add) - always
--   2. transaction_executed (transaction_date) - business event
--   3. transaction_settled (settlement_date) - when exists and != transaction_date
--   4. transaction_updated (rec_edit) - when exists and != rec_add
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS org_123.mv_redtail_transaction_to_timeline
TO org_123.timeline
AS
-- Event 1: transaction_created [timestamp: rec_add]
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'transaction_created' AS event_type,
    rec_add AS timestamp,
    'rec_add' AS event_timestamp_source,
    'transaction' AS entity_type,
    concat('transaction_', toString(rec_id)) AS entity_id,
    concat(COALESCE(transaction_type, 'Transaction'), ' transaction recorded') AS description,
    'redtail' AS source_system,
    'redtail_silver.transaction' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'transaction_number', COALESCE(transaction_number, ''),
        'transaction_type', COALESCE(transaction_type, ''),
        'account_id', toString(COALESCE(account_id, 0)),
        'investment_id', toString(COALESCE(investment_id, 0)),
        'amount', toString(COALESCE(amount, 0)),
        'backfill', 'true'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.transaction
WHERE rec_id IS NOT NULL
  AND rec_add IS NOT NULL
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681'

UNION ALL

-- Event 2: transaction_executed [timestamp: transaction_date] - business event
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'transaction_executed' AS event_type,
    transaction_date AS timestamp,
    'transaction_date' AS event_timestamp_source,
    'transaction' AS entity_type,
    concat('transaction_', toString(rec_id)) AS entity_id,
    concat(COALESCE(transaction_type, 'Transaction'), ' transaction: ', toString(COALESCE(quantity, 0)), ' units at $', toString(COALESCE(price_per_unit, 0)), ' - Total: $', toString(COALESCE(amount, 0))) AS description,
    'redtail' AS source_system,
    'redtail_silver.transaction' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'transaction_number', COALESCE(transaction_number, ''),
        'transaction_type', COALESCE(transaction_type, ''),
        'account_id', toString(COALESCE(account_id, 0)),
        'investment_id', toString(COALESCE(investment_id, 0)),
        'amount', toString(COALESCE(amount, 0)),
        'fees', toString(COALESCE(fees, 0)),
        'quantity', toString(COALESCE(quantity, 0)),
        'price_per_unit', toString(COALESCE(price_per_unit, 0)),
        'currency', COALESCE(currency, 'USD'),
        'status', COALESCE(status_normalized, ''),
        'reference_number', COALESCE(reference_number, ''),
        'backfill', 'true'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.transaction
WHERE rec_id IS NOT NULL
  AND transaction_date IS NOT NULL
  AND (rec_add IS NULL OR transaction_date != rec_add)
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681'

UNION ALL

-- Event 3: transaction_settled [timestamp: settlement_date] - when settlement_date != transaction_date
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'transaction_settled' AS event_type,
    settlement_date AS timestamp,
    'settlement_date' AS event_timestamp_source,
    'transaction' AS entity_type,
    concat('transaction_', toString(rec_id)) AS entity_id,
    concat(COALESCE(transaction_type, 'Transaction'), ' transaction settled - $', toString(COALESCE(amount, 0))) AS description,
    'redtail' AS source_system,
    'redtail_silver.transaction' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'transaction_number', COALESCE(transaction_number, ''),
        'transaction_type', COALESCE(transaction_type, ''),
        'account_id', toString(COALESCE(account_id, 0)),
        'investment_id', toString(COALESCE(investment_id, 0)),
        'amount', toString(COALESCE(amount, 0)),
        'status', COALESCE(status_normalized, ''),
        'backfill', 'true'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.transaction
WHERE rec_id IS NOT NULL
  AND settlement_date IS NOT NULL
  AND (transaction_date IS NULL OR settlement_date != transaction_date)
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681'

UNION ALL

-- Event 4: transaction_updated [timestamp: rec_edit] - when rec_edit != rec_add
SELECT
    generateUUIDv4() AS event_id,
    glynac_organization_id AS org_id,
    'transaction_updated' AS event_type,
    rec_edit AS timestamp,
    'rec_edit' AS event_timestamp_source,
    'transaction' AS entity_type,
    concat('transaction_', toString(rec_id)) AS entity_id,
    concat('Transaction updated: ', COALESCE(transaction_number, 'Unknown')) AS description,
    'redtail' AS source_system,
    'redtail_silver.transaction' AS source_table,
    toString(rec_id) AS source_id,
    CAST(NULL AS Nullable(String)) AS minio_path,
    rec_add AS rec_add,
    rec_edit AS rec_edit,
    toJSONString(map(
        'transaction_type', COALESCE(transaction_type, ''),
        'status', COALESCE(status_normalized, ''),
        'rec_edit_user', toString(COALESCE(rec_edit_user, 0)),
        'backfill', 'true',
        'represents', 'final_state'
    )) AS metadata,
    processing_date,
    now() AS _loaded_at,
    1 AS _version
FROM redtail_silver.transaction
WHERE rec_id IS NOT NULL
  AND rec_edit IS NOT NULL
  AND rec_add IS NOT NULL
  AND rec_edit != rec_add
  AND glynac_organization_id = '29a436a3-b5de-4afd-9c7a-059246c5a681';
