SELECT
    -- Portfolio group identifier
    trimBoth(COALESCE(id, '')) as id,
    
    -- Portfolio group name
    trimBoth(COALESCE(name, '')) as name,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.portfolio_groups_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.portfolio_groups_raw
WHERE id IS NOT NULL
  AND id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, id, processing_date 
    ORDER BY COALESCE(name, '') DESC
) = 1