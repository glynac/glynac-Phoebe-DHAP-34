SELECT
    -- Target component identifiers
    id,
    trimBoth(COALESCE(target_id, '')) as target_id,
    toFloat64OrZero(asset_id) as asset_id,
    
    -- Component classification
    trimBoth(COALESCE(component_name, '')) as component_name,
    trimBoth(COALESCE(class, '')) as class,
    trimBoth(COALESCE(segment, '')) as segment,
    
    -- Allocation and tolerance
    toFloat64OrZero(allocation) as allocation,
    toFloat64OrZero(tolerance_lower) as tolerance_lower,
    toFloat64OrZero(tolerance_upper) as tolerance_upper,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.standalone_target_components_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.standalone_target_components_raw
WHERE id IS NOT NULL
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, id 
    ORDER BY processing_date DESC
) = 1