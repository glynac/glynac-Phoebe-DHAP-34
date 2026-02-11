SELECT
    -- Holding identifier
    trimBoth(COALESCE(id, '')) as id,
    
    -- Account and asset references
    trimBoth(COALESCE(account_id, '')) as account_id,
    asset_id,
    
    -- As-of date
    parseDateTime64BestEffortOrNull(toString(as_of_date)) as as_of_date,
    
    -- Position details
    COALESCE(units, 0.0) as units,
    COALESCE(market_value, 0.0) as market_value,
    
    -- Holding flags
    COALESCE(discretionary, false) as discretionary,
    COALESCE(supervised, false) as supervised,
    COALESCE(billable, false) as billable,
    COALESCE(money_market, false) as money_market,
    COALESCE(cash, false) as cash,
    COALESCE(long_holding, false) as long_holding,
    COALESCE(is_manual, false) as is_manual,
    
    -- Discretion and authority
    trimBoth(COALESCE(investment_discretion, '')) as investment_discretion,
    trimBoth(COALESCE(voting_authority, '')) as voting_authority,
    
    -- Pricing factors
    COALESCE(price_factor, 1.0) as price_factor,
    COALESCE(paydown_factor, 1.0) as paydown_factor,
    
    -- Fixed income attributes
    coupon_rate,
    muni_state,
    trimBoth(COALESCE(call_date, '')) as call_date,
    yield,
    trimBoth(COALESCE(maturity_date, '')) as maturity_date,
    COALESCE(accrual, 0.0) as accrual,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.holdings_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.holdings_raw
WHERE id IS NOT NULL
  AND id != ''
  AND account_id IS NOT NULL
  AND account_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(as_of_date)) DESC
) = 1