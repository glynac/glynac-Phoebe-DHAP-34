SELECT
    -- Asset identifier
    asset_id,
    
    -- Tag name and value
    trimBoth(COALESCE(name, '')) as name,
    trimBoth(COALESCE(value, '')) as value,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.asset_tags_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.asset_tags_raw
WHERE asset_id IS NOT NULL
  AND name IS NOT NULL
  AND name != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, asset_id, name, processing_date 
    ORDER BY value DESC
) = 1