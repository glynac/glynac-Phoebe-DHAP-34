SELECT
    -- Client-account relationship identifiers
    user_id,
    trimBoth(COALESCE(account_number, '')) as account_number,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.client_accounts_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.client_accounts_raw
WHERE user_id IS NOT NULL
  AND account_number IS NOT NULL
  AND account_number != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, user_id, account_number 
    ORDER BY processing_date DESC
) = 1