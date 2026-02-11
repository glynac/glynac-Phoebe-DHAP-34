SELECT
    -- Portfolio identifier
    trimBoth(COALESCE(id, '')) as id,
    
    -- Portfolio names
    trimBoth(COALESCE(name, '')) as name,
    trimBoth(COALESCE(display_name, '')) as display_name,
    
    -- Relationship references
    relationship_id,
    trimBoth(COALESCE(external_relationship_id, '')) as external_relationship_id,
    
    -- Portfolio flags
    COALESCE(is_master_portfolio, false) as is_master_portfolio,
    COALESCE(exclude_from_rebalancing, false) as exclude_from_rebalancing,
    COALESCE(admin_only, false) as admin_only,
    COALESCE(demo_portfolio, false) as demo_portfolio,
    COALESCE(history_loaded, false) as history_loaded,
    COALESCE(client_visibility, false) as client_visibility,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.portfolios_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.portfolios_raw
WHERE id IS NOT NULL
  AND id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, id, processing_date 
    ORDER BY COALESCE(name, '') DESC
) = 1