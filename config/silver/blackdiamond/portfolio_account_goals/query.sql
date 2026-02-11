SELECT
    -- Portfolio and account references
    trimBoth(COALESCE(portfolio_id, '')) as portfolio_id,
    trimBoth(COALESCE(account_id, '')) as account_id,
    
    -- Goal reference and ordering
    goal_id,
    ordinal,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.portfolio_account_goals_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.portfolio_account_goals_raw
WHERE portfolio_id IS NOT NULL
  AND portfolio_id != ''
  AND account_id IS NOT NULL
  AND account_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, portfolio_id, account_id, processing_date 
    ORDER BY COALESCE(ordinal, 999) ASC, COALESCE(goal_id, 0) DESC
) = 1