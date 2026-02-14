SELECT
    -- Target model identification
    trimBoth(COALESCE(target_id, '')) as target_id,
    trimBoth(COALESCE(composition, '')) as composition,
    
    -- Allocation percentage
    COALESCE(allocation, 0.0) as allocation,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.target_components_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.target_components_raw
WHERE target_id IS NOT NULL
  AND target_id != ''
  AND composition IS NOT NULL
  AND composition != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, target_id, composition, processing_date 
    ORDER BY allocation DESC NULLS LAST
) = 1