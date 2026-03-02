SELECT
    -- Goal identifiers
    goal_id,
    trimBoth(COALESCE(goal_external_id, '')) as goal_external_id,
    trimBoth(COALESCE(goal_name, '')) as goal_name,
    goal_ordinal,
    
    -- Benchmark information
    trimBoth(COALESCE(benchmark_external_id, '')) as benchmark_external_id,
    trimBoth(COALESCE(benchmark_long_name, '')) as benchmark_long_name,
    trimBoth(COALESCE(benchmark_short_name, '')) as benchmark_short_name,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.standalone_goals_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.standalone_goals_raw
WHERE goal_id IS NOT NULL
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, goal_id 
    ORDER BY processing_date DESC
) = 1