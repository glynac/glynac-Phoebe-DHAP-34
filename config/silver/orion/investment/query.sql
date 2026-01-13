SELECT
    investment_id,
    glynac_organization_id,
    
    -- Investment classification
    trimBoth(COALESCE(record_type, '')) as record_type,
    trimBoth(COALESCE(investment_external_id, '')) as investment_external_id,
    trimBoth(COALESCE(investment_type, '')) as investment_type,
    trimBoth(COALESCE(investment_status, '')) as investment_status,
    
    -- Relationships
    trimBoth(COALESCE(account_id, '')) as account_id,
    trimBoth(COALESCE(portfolio_id, '')) as portfolio_id,
    trimBoth(COALESCE(asset_id, '')) as asset_id,
    trimBoth(COALESCE(asset_name, '')) as asset_name,
    
    -- Investment details
    toDecimal64OrZero(amount, 2) as amount,
    trimBoth(COALESCE(currency, '')) as currency,
    toDecimal64OrZero(quantity, 4) as quantity,
    toDecimal64OrZero(purchase_price, 4) as purchase_price,
    toDecimal64OrZero(current_price, 4) as current_price,
    toDecimal64OrZero(current_value, 2) as current_value,
    
    -- Performance metrics
    toDecimal64OrZero(gain_loss, 2) as gain_loss,
    toDecimal64OrZero(gain_loss_percentage, 2) as gain_loss_percentage,
    toDecimal64OrZero(yield_rate, 4) as yield_rate,
    
    -- Risk assessment
    trimBoth(COALESCE(risk_rating, '')) as risk_rating,
    
    -- Tax and dates
    if(is_tax_advantaged = true, 1, 0) as is_tax_advantaged,
    parseDateTime64BestEffortOrNull(toString(purchase_date)) as purchase_date,
    parseDateTime64BestEffortOrNull(toString(maturity_date)) as maturity_date,
    parseDateTime64BestEffortOrNull(toString(created_date)) as created_date,
    parseDateTime64BestEffortOrNull(toString(modified_date)) as modified_date,
    
    -- Additional information
    trimBoth(COALESCE(tags, '')) as tags,
    
    -- Batch metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'orion.investment_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM orion.investment_raw
WHERE investment_id IS NOT NULL
  AND investment_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY investment_id, glynac_organization_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1