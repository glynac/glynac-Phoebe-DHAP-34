SELECT
    -- Account and class identifiers
    trimBoth(COALESCE(account_id, '')) as account_id,
    class_id,
    trimBoth(COALESCE(class_name, '')) as class_name,
    
    -- As-of date
    parseDateTime64BestEffortOrNull(toString(as_of_date)) as as_of_date,
    
    -- Market values
    COALESCE(total_emv, 0.0) as total_emv,
    COALESCE(supervised_emv, 0.0) as supervised_emv,
    COALESCE(unsupervised_emv, 0.0) as unsupervised_emv,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.class_allocations_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.class_allocations_raw
WHERE account_id IS NOT NULL
  AND account_id != ''
  AND class_id IS NOT NULL
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, account_id, class_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(as_of_date)) DESC
) = 1