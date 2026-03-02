SELECT
    -- Portfolio and goal identifiers
    trimBoth(COALESCE(portfolio_id, '')) as portfolio_id,
    goal_id,
    goal_ordinal,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.portfolio_goal_assignments_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.portfolio_goal_assignments_raw
WHERE portfolio_id IS NOT NULL
  AND portfolio_id != ''
  AND goal_id IS NOT NULL
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, portfolio_id, goal_id 
    ORDER BY processing_date DESC
) = 1