SELECT
    position_record_id,
    glynac_organization_id,
    
    -- Position classification
    trimBoth(COALESCE(position_type_record, '')) as position_type_record,
    trimBoth(COALESCE(position_id, '')) as position_id,
    trimBoth(COALESCE(position_type, '')) as position_type,
    trimBoth(COALESCE(position_status, '')) as position_status,
    
    -- Relationships
    trimBoth(COALESCE(portfolio_id, '')) as portfolio_id,
    trimBoth(COALESCE(account_id, '')) as account_id,
    trimBoth(COALESCE(asset_id, '')) as asset_id,
    
    -- Asset details
    trimBoth(COALESCE(symbol, '')) as symbol,
    trimBoth(COALESCE(asset_name, '')) as asset_name,
    
    -- Quantity and pricing
    toDecimal64OrZero(quantity, 4) as quantity,
    toDecimal64OrZero(cost_basis, 2) as cost_basis,
    toDecimal64OrZero(cost_basis_per_share, 4) as cost_basis_per_share,
    toDecimal64OrZero(current_price, 4) as current_price,
    toDecimal64OrZero(market_value, 2) as market_value,
    trimBoth(COALESCE(currency, '')) as currency,
    toDecimal64OrZero(average_price, 4) as average_price,
    
    -- Performance metrics
    toDecimal64OrZero(unrealized_gain_loss, 2) as unrealized_gain_loss,
    toDecimal64OrZero(unrealized_gain_loss_percent, 2) as unrealized_gain_loss_percent,
    toDecimal64OrZero(allocation_percentage, 2) as allocation_percentage,
    
    -- Holding period
    parseDateTime64BestEffortOrNull(toString(open_date)) as open_date,
    toInt32OrZero(days_held) as days_held,
    
    -- Additional information
    trimBoth(COALESCE(tags, '')) as tags,
    
    -- Dates
    parseDateTime64BestEffortOrNull(toString(created_date)) as created_date,
    parseDateTime64BestEffortOrNull(toString(modified_date)) as modified_date,
    
    -- Batch metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'orion.finance_position_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM orion.finance_position_raw
WHERE position_record_id IS NOT NULL
  AND position_record_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY position_record_id, glynac_organization_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1