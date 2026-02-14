SELECT
    -- Target identifier
    trimBoth(COALESCE(id, '')) as id,
    
    -- Assignment
    trimBoth(COALESCE(portfolio_id, '')) as portfolio_id,
    account_id,
    
    -- Target details
    trimBoth(COALESCE(name, '')) as name,
    trimBoth(COALESCE(description, '')) as description,
    
    -- Date range
    toDate(parseDateTime64BestEffortOrNull(toString(start_date))) as start_date,
    toDate(parseDateTime64BestEffortOrNull(toString(end_date))) as end_date,
    
    -- Hierarchy
    target_level,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.targets_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.targets_raw
WHERE id IS NOT NULL
  AND id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, id, processing_date 
    ORDER BY toDate(parseDateTime64BestEffortOrNull(toString(start_date))) DESC NULLS LAST
) = 1