SELECT
    rec_id,
    glynac_organization_id,
    
    -- Investment identifiers
    trimBoth(COALESCE(investment_name, '')) as investment_name,
    trimBoth(COALESCE(symbol, '')) as symbol,
    trimBoth(COALESCE(cusip, '')) as cusip,
    trimBoth(COALESCE(isin, '')) as isin,
    
    -- Investment classification
    trimBoth(COALESCE(investment_type, '')) as investment_type,
    trimBoth(COALESCE(asset_class, '')) as asset_class,
    trimBoth(COALESCE(category, '')) as category,
    
    -- Current pricing (convert string to Decimal)
    CAST(
        replaceAll(replaceAll(COALESCE(current_price, '0'), ',', ''), '$', '') 
        AS Decimal(15, 4)
    ) as current_price,
    
    CAST(
        replaceAll(replaceAll(COALESCE(previous_close, '0'), ',', ''), '$', '') 
        AS Decimal(15, 4)
    ) as previous_close,
    
    CAST(
        replaceAll(replaceAll(COALESCE(price_change, '0'), ',', ''), '$', '') 
        AS Decimal(15, 4)
    ) as price_change,
    
    CAST(
        replaceAll(replaceAll(COALESCE(price_change_percent, '0'), ',', ''), '%', '') 
        AS Decimal(8, 2)
    ) as price_change_percent,
    
    -- Description and metadata
    trimBoth(COALESCE(description, '')) as description,
    trimBoth(COALESCE(currency, 'USD')) as currency,
    trimBoth(COALESCE(country, '')) as country,
    trimBoth(COALESCE(exchange, '')) as exchange,
    trimBoth(COALESCE(sector, '')) as sector,
    trimBoth(COALESCE(industry, '')) as industry,
    
    -- Bond-specific fields
    CAST(
        replaceAll(replaceAll(COALESCE(coupon_rate, '0'), ',', ''), '%', '') 
        AS Decimal(6, 3)
    ) as coupon_rate,
    
    parseDateTime64BestEffortOrNull(toString(maturity_date)) as maturity_date,
    trimBoth(COALESCE(credit_rating, '')) as credit_rating,
    
    -- Fund-specific fields
    CAST(
        replaceAll(replaceAll(COALESCE(expense_ratio, '0'), ',', ''), '%', '') 
        AS Decimal(6, 4)
    ) as expense_ratio,
    
    trimBoth(COALESCE(fund_manager, '')) as fund_manager,
    trimBoth(COALESCE(fund_family, '')) as fund_family,
    
    CAST(
        replaceAll(replaceAll(COALESCE(fund_aum, '0'), ',', ''), '$', '') 
        AS Decimal(20, 2)
    ) as fund_aum,
    
    -- Performance metrics (convert string percentages to Decimal)
    CAST(
        replaceAll(replaceAll(COALESCE(ytd_return, '0'), ',', ''), '%', '') 
        AS Decimal(6, 2)
    ) as ytd_return,
    
    CAST(
        replaceAll(replaceAll(COALESCE(one_year_return, '0'), ',', ''), '%', '') 
        AS Decimal(6, 2)
    ) as one_year_return,
    
    CAST(
        replaceAll(replaceAll(COALESCE(three_year_return, '0'), ',', ''), '%', '') 
        AS Decimal(6, 2)
    ) as three_year_return,
    
    CAST(
        replaceAll(replaceAll(COALESCE(five_year_return, '0'), ',', ''), '%', '') 
        AS Decimal(6, 2)
    ) as five_year_return,
    
    CAST(
        replaceAll(replaceAll(COALESCE(dividend_yield, '0'), ',', ''), '%', '') 
        AS Decimal(6, 2)
    ) as dividend_yield,
    
    -- Flags (convert Bool to UInt8)
    CAST(is_active AS UInt8) as is_active,
    
    -- Audit fields
    parseDateTime64BestEffortOrNull(toString(rec_add)) as rec_add,
    rec_add_user,
    parseDateTime64BestEffortOrNull(toString(rec_edit)) as rec_edit,
    rec_edit_user,
    
    -- Batch metadata
    trimBoth(COALESCE(scan_id, '')) as scan_id,
    batch_number,
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'redtail.investment_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM redtail.investment_raw
WHERE rec_id IS NOT NULL
  AND rec_id != 0
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rec_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC, batch_number DESC 
) = 1