SELECT
    -- Benchmark identifier
    trimBoth(COALESCE(benchmark_id, '')) as benchmark_id,
    
    -- Benchmark name
    trimBoth(COALESCE(name, '')) as name,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.benchmarks_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.benchmarks_raw
WHERE benchmark_id IS NOT NULL
  AND benchmark_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, benchmark_id, processing_date 
    ORDER BY name DESC
) = 1