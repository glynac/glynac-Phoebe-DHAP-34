SELECT
    -- Portfolio group and portfolio references
    trimBoth(COALESCE(portfolio_group_id, '')) as portfolio_group_id,
    trimBoth(COALESCE(portfolio_id, '')) as portfolio_id,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.portfolio_group_members_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.portfolio_group_members_raw
WHERE portfolio_group_id IS NOT NULL
  AND portfolio_group_id != ''
  AND portfolio_id IS NOT NULL
  AND portfolio_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, portfolio_group_id, portfolio_id, processing_date 
    ORDER BY portfolio_id DESC
) = 1