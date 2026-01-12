SELECT
    asset_id,
    glynac_organization_id,
    
    -- Asset identification
    trimBoth(COALESCE(asset_record_id, '')) as asset_record_id,
    trimBoth(COALESCE(asset_type_field, '')) as asset_type_field,
    trimBoth(COALESCE(symbol, '')) as symbol,
    trimBoth(COALESCE(asset_name, '')) as asset_name,
    
    -- Asset classification
    trimBoth(COALESCE(asset_class, '')) as asset_class,
    trimBoth(COALESCE(asset_subclass, '')) as asset_subclass,
    trimBoth(COALESCE(asset_type, '')) as asset_type,
    
    -- Pricing information
    COALESCE(current_price, 0.0) as current_price,
    trimBoth(COALESCE(price_currency, '')) as price_currency,
    trimBoth(COALESCE(exchange, '')) as exchange,
    
    -- Market data
    trimBoth(COALESCE(sector, '')) as sector,
    trimBoth(COALESCE(industry, '')) as industry,
    COALESCE(market_cap, 0) as market_cap,
    
    -- Financial metrics
    COALESCE(pe_ratio, 0.0) as pe_ratio,
    COALESCE(dividend_yield, 0.0) as dividend_yield,
    COALESCE(beta, 0.0) as beta,
    COALESCE(fifty_two_week_high, 0.0) as fifty_two_week_high,
    COALESCE(fifty_two_week_low, 0.0) as fifty_two_week_low,
    
    -- Additional details
    trimBoth(COALESCE(country, '')) as country,
    trimBoth(COALESCE(issuer, '')) as issuer,
    trimBoth(COALESCE(maturity_date, '')) as maturity_date,
    COALESCE(coupon_rate, 0.0) as coupon_rate,
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
    'orion.asset_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM orion.asset_raw
WHERE asset_id IS NOT NULL
  AND asset_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY asset_id, glynac_organization_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1
