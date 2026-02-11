SELECT
    -- Goal identifier
    goal_id,
    
    -- Portfolio reference
    trimBoth(COALESCE(portfolio_id, '')) as portfolio_id,
    
    -- Goal details
    trimBoth(COALESCE(name, '')) as name,
    ordinal,
    
    -- Benchmark reference
    benchmark_id,
    trimBoth(COALESCE(benchmark_name, '')) as benchmark_name,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.goals_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.goals_raw
WHERE goal_id IS NOT NULL
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, goal_id, processing_date 
    ORDER BY COALESCE(ordinal, 999) ASC, COALESCE(name, '') DESC
) = 1