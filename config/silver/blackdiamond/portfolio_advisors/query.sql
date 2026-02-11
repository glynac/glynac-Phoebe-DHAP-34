SELECT
    -- Portfolio and advisor references
    trimBoth(COALESCE(portfolio_id, '')) as portfolio_id,
    trimBoth(COALESCE(advisor_id, '')) as advisor_id,
    
    -- Advisor name
    trimBoth(COALESCE(first_name, '')) as first_name,
    trimBoth(COALESCE(last_name, '')) as last_name,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.portfolio_advisors_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.portfolio_advisors_raw
WHERE portfolio_id IS NOT NULL
  AND portfolio_id != ''
  AND advisor_id IS NOT NULL
  AND advisor_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, portfolio_id, advisor_id, processing_date 
    ORDER BY COALESCE(last_name, '') DESC, COALESCE(first_name, '') DESC
) = 1