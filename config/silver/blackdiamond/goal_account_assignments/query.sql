SELECT
    -- Goal and portfolio identifiers
    goal_id,
    trimBoth(COALESCE(portfolio_id, '')) as portfolio_id,
    
    -- Account information
    trimBoth(COALESCE(account_number, '')) as account_number,
    trimBoth(COALESCE(account_name, '')) as account_name,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.goal_account_assignments_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.goal_account_assignments_raw
WHERE goal_id IS NOT NULL
  AND account_number IS NOT NULL
  AND account_number != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, goal_id, account_number 
    ORDER BY processing_date DESC
) = 1