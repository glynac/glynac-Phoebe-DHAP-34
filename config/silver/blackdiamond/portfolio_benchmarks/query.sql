SELECT
    -- Portfolio and benchmark references
    trimBoth(COALESCE(portfolio_id, '')) as portfolio_id,
    trimBoth(COALESCE(benchmark_id, '')) as benchmark_id,
    
    -- Effective date range
    parseDateTime64BestEffortOrNull(toString(start_date)) as start_date,
    parseDateTime64BestEffortOrNull(toString(end_date)) as end_date,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.portfolio_benchmarks_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.portfolio_benchmarks_raw
WHERE portfolio_id IS NOT NULL
  AND portfolio_id != ''
  AND benchmark_id IS NOT NULL
  AND benchmark_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, portfolio_id, benchmark_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(start_date)) DESC
) = 1