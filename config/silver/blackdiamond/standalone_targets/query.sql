SELECT
    -- Target identifiers and metadata
    trimBoth(COALESCE(id, '')) as id,
    trimBoth(COALESCE(name, '')) as name,
    trimBoth(COALESCE(description, '')) as description,
    trimBoth(COALESCE(target_type, '')) as target_type,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.standalone_targets_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.standalone_targets_raw
WHERE id IS NOT NULL
  AND id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, id 
    ORDER BY processing_date DESC
) = 1