SELECT
    -- Transaction identifier
    CAST(transaction_id as Int64) as transaction_id,
    
    -- Account and holding references
    trimBoth(COALESCE(holding_id, '')) as holding_id,
    trimBoth(COALESCE(account_id, '')) as account_id,
    trimBoth(COALESCE(account_number, '')) as account_number,
    
    -- Asset identification
    asset_id,
    trimBoth(COALESCE(ticker, '')) as ticker,
    trimBoth(COALESCE(cusip, '')) as cusip,
    trimBoth(COALESCE(display_cusip, '')) as display_cusip,
    trimBoth(COALESCE(alternate_id, '')) as alternate_id,
    
    -- Financial values
    COALESCE(market_value, 0.0) as market_value,
    COALESCE(units, 0.0) as units,
    COALESCE(price, 0.0) as price,
    transaction_fee,
    account_fee,
    
    -- Transaction codes and classification
    trimBoth(COALESCE(sub_code, '')) as sub_code,
    trimBoth(COALESCE(trans_code, '')) as trans_code,
    trimBoth(COALESCE(description, '')) as description,
    trimBoth(COALESCE(file_code_description, '')) as file_code_description,
    
    -- Dates
    toDate(parseDateTime64BestEffortOrNull(toString(return_date))) as return_date,
    toDate(parseDateTime64BestEffortOrNull(toString(settle_date))) as settle_date,
    toDate(parseDateTime64BestEffortOrNull(toString(trade_date))) as trade_date,
    
    -- Transaction type and details
    trimBoth(COALESCE(transaction_type, '')) as transaction_type,
    trimBoth(COALESCE(transaction_sub_type, '')) as transaction_sub_type,
    external_contra_account_id,
    trimBoth(COALESCE(notes, '')) as notes,
    trimBoth(COALESCE(action, '')) as action,
    trimBoth(COALESCE(external_flow_affect, '')) as external_flow_affect,
    trimBoth(COALESCE(issue_type, '')) as issue_type,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.transactions_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.transactions_raw
WHERE transaction_id IS NOT NULL
  AND account_id IS NOT NULL
  AND account_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, transaction_id, processing_date 
    ORDER BY toDate(parseDateTime64BestEffortOrNull(toString(trade_date))) DESC NULLS LAST,
             toDate(parseDateTime64BestEffortOrNull(toString(settle_date))) DESC NULLS LAST
) = 1