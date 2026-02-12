SELECT
    -- Mapping keys
    trimBoth(COALESCE(relationship_id, '')) as relationship_id,
    trimBoth(COALESCE(portfolio_id, '')) as portfolio_id,
    
    -- Flags
    COALESCE(is_primary, false) as is_primary,
    COALESCE(client_visible, false) as client_visible,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.relationship_portfolios_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.relationship_portfolios_raw
WHERE relationship_id IS NOT NULL
  AND relationship_id != ''
  AND portfolio_id IS NOT NULL
  AND portfolio_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, relationship_id, portfolio_id, processing_date 
    ORDER BY is_primary DESC, client_visible DESC
) = 1