-- query.sql
SELECT
    -- Target and portfolio identifiers
    trimBoth(COALESCE(target_id, '')) as target_id,
    trimBoth(COALESCE(portfolio_id, '')) as portfolio_id,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.target_portfolios_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.target_portfolios_raw
WHERE target_id IS NOT NULL
  AND target_id != ''
  AND portfolio_id IS NOT NULL
  AND portfolio_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, target_id, portfolio_id 
    ORDER BY processing_date DESC
) = 1