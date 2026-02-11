SELECT
    -- Account identifier
    trimBoth(COALESCE(account_id, '')) as account_id,
    
    -- As-of date
    parseDateTime64BestEffortOrNull(toString(as_of_date)) as as_of_date,
    
    -- Tax status
    tax_status,
    
    -- Market value and units
    COALESCE(emv, 0.0) as emv,
    COALESCE(units, 0) as units,
    
    -- Cost basis and proceeds
    COALESCE(cost_basis, 0.0) as cost_basis,
    proceeds,
    
    -- Realized gains/losses (long and short term)
    COALESCE(realized_lt, 0.0) as realized_lt,
    COALESCE(realized_st, 0.0) as realized_st,
    
    -- Unrealized gains/losses (long and short term)
    COALESCE(unrealized_lt, 0.0) as unrealized_lt,
    COALESCE(unrealized_st, 0.0) as unrealized_st,
    
    -- Accrual
    COALESCE(accrual, 0.0) as accrual,
    
    -- Year-to-date realized gains
    ytd_short_term_realized_gain,
    ytd_long_term_realized_gain,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.cost_basis_summary_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.cost_basis_summary_raw
WHERE account_id IS NOT NULL
  AND account_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, account_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(as_of_date)) DESC
) = 1