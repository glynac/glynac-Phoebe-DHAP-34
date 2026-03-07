SELECT
    -- Portfolio and account references
    trimBoth(COALESCE(portfolio_id, '')) as portfolio_id,
    trimBoth(COALESCE(account_number, '')) as account_number,
    trimBoth(COALESCE(account_name, '')) as account_name,

    -- Organization and partition keys
    glynac_organization_id,
    processing_date,

    -- System columns
    now() as _loaded_at,
    'blackdiamond.portfolio_accounts_not_assigned_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.portfolio_accounts_not_assigned_raw
WHERE portfolio_id IS NOT NULL
  AND portfolio_id != ''
  AND account_number IS NOT NULL
  AND account_number != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, portfolio_id, account_number, processing_date
    ORDER BY processing_date DESC
) = 1
