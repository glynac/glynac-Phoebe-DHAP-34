SELECT
    -- Identifiers
    trimBoth(COALESCE(tax_lot_id, '')) as tax_lot_id,
    trimBoth(COALESCE(holding_id, '')) as holding_id,
    trimBoth(COALESCE(account_id, '')) as account_id,
    asset_id,
    
    -- Asset identification
    trimBoth(COALESCE(ticker, '')) as ticker,
    trimBoth(COALESCE(cusip, '')) as cusip,
    trimBoth(COALESCE(display_cusip, '')) as display_cusip,
    trimBoth(COALESCE(asset_name, '')) as asset_name,
    trimBoth(COALESCE(alternative_identifier, '')) as alternative_identifier,
    
    -- Dates
    toDate(parseDateTime64BestEffortOrNull(toString(trade_date))) as trade_date,
    toDate(parseDateTime64BestEffortOrNull(toString(open_date))) as open_date,
    
    -- Tax and status
    tax_status,
    
    -- Financial values
    COALESCE(emv, 0.0) as emv,
    COALESCE(units, 0.0) as units,
    COALESCE(cost_basis, 0.0) as cost_basis,
    COALESCE(proceeds, 0.0) as proceeds,
    COALESCE(realized_lt, 0.0) as realized_lt,
    COALESCE(realized_st, 0.0) as realized_st,
    COALESCE(unrealized_lt, 0.0) as unrealized_lt,
    COALESCE(unrealized_st, 0.0) as unrealized_st,
    COALESCE(unit_cost, 0.0) as unit_cost,
    
    -- Additional fields
    accrual,
    COALESCE(price_factor, 1.0) as price_factor,
    COALESCE(paydown_factor, 1.0) as paydown_factor,
    ordinal,
    COALESCE(cash, false) as cash,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.tax_lots_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.tax_lots_raw
WHERE tax_lot_id IS NOT NULL
  AND tax_lot_id != ''
  AND account_id IS NOT NULL
  AND account_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, tax_lot_id, processing_date 
    ORDER BY toDate(parseDateTime64BestEffortOrNull(toString(open_date))) DESC NULLS LAST,
             ordinal DESC NULLS LAST
) = 1