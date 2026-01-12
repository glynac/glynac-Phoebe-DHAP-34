SELECT
    asset_record_id,
    glynac_organization_id,
    
    -- Record classification
    trimBoth(COALESCE(asset_record_type, '')) as asset_record_type,
    trimBoth(COALESCE(asset_id, '')) as asset_id,
    
    -- Basic identifiers
    trimBoth(COALESCE(symbol, '')) as symbol,
    trimBoth(COALESCE(name, '')) as name,
    
    -- Classification
    trimBoth(COALESCE(asset_class, '')) as asset_class,
    trimBoth(COALESCE(asset_subclass, '')) as asset_subclass,
    trimBoth(COALESCE(asset_type, '')) as asset_type,
    
    -- Pricing information
    toDecimal64OrZero(current_price, 4) as current_price,
    trimBoth(COALESCE(price_currency, '')) as price_currency,
    trimBoth(COALESCE(exchange, '')) as exchange,
    
    -- Company/issuer information
    trimBoth(COALESCE(sector, '')) as sector,
    trimBoth(COALESCE(industry, '')) as industry,
    trimBoth(COALESCE(country, '')) as country,
    trimBoth(COALESCE(issuer, '')) as issuer,
    
    -- Market metrics (for equities)
    toDecimal64OrZero(market_cap, 2) as market_cap,
    toDecimal64OrZero(pe_ratio, 2) as pe_ratio,
    toDecimal64OrZero(dividend_yield, 4) as dividend_yield,
    toDecimal64OrZero(beta, 4) as beta,
    toDecimal64OrZero(fifty_two_week_high, 4) as fifty_two_week_high,
    toDecimal64OrZero(fifty_two_week_low, 4) as fifty_two_week_low,
    
    -- Bond-specific fields
    parseDateTime64BestEffortOrNull(toString(maturity_date)) as maturity_date,
    toDecimal64OrZero(coupon_rate, 4) as coupon_rate,
    
    -- Metadata
    trimBoth(COALESCE(tags, '')) as tags,
    trimBoth(COALESCE(description, '')) as description,
    
    -- Dates (parse string to DateTime)
    parseDateTime64BestEffortOrNull(toString(created_date)) as created_date,
    parseDateTime64BestEffortOrNull(toString(modified_date)) as modified_date,
    
    -- Batch metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'orion.finance_asset_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM orion.finance_asset_raw
WHERE asset_record_id IS NOT NULL
  AND asset_record_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY asset_record_id, glynac_organization_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1