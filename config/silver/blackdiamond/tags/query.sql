SELECT
    -- Entity identification
    trimBoth(COALESCE(entity_type, '')) as entity_type,
    trimBoth(COALESCE(entity_id, '')) as entity_id,
    
    -- Tag key-value
    trimBoth(COALESCE(name, '')) as name,
    trimBoth(COALESCE(value, '')) as value,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.tags_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.tags_raw
WHERE entity_type IS NOT NULL
  AND entity_type != ''
  AND entity_id IS NOT NULL
  AND entity_id != ''
  AND name IS NOT NULL
  AND name != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, entity_type, entity_id, name, processing_date 
    ORDER BY value DESC NULLS LAST
) = 1