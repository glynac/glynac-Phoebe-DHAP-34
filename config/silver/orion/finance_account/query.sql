SELECT
    finance_account_id,
    glynac_organization_id,
    
    -- Account identifiers
    trimBoth(COALESCE(account_number, '')) as account_number,
    trimBoth(COALESCE(account_number_masked, '')) as account_number_masked,
    trimBoth(COALESCE(account_name, '')) as account_name,
    
    -- Account classification
    trimBoth(COALESCE(account_type, '')) as account_type,
    trimBoth(COALESCE(category, '')) as category,
    trimBoth(COALESCE(account_status, '')) as account_status,
    
    -- Financial details (convert Float64 to Decimal)
    trimBoth(COALESCE(currency, '')) as currency,
    toDecimal64OrZero(balance, 2) as balance,
    toDecimal64OrZero(available_balance, 2) as available_balance,
    toDecimal64OrZero(pending_balance, 2) as pending_balance,
    toDecimal64OrZero(interest_rate, 4) as interest_rate,
    
    -- Relationships
    trimBoth(COALESCE(account_id, '')) as account_id,
    trimBoth(COALESCE(organization_id, '')) as organization_id,
    trimBoth(COALESCE(owner_user_id, '')) as owner_user_id,
    trimBoth(COALESCE(customer_id, '')) as customer_id,
    
    -- Institution details (PII)
    trimBoth(COALESCE(institution, '')) as institution,
    trimBoth(COALESCE(routing_number, '')) as routing_number,
    
    -- Settings
    trimBoth(COALESCE(settings, '')) as settings,
    
    -- Dates (parse string to DateTime)
    parseDateTime64BestEffortOrNull(toString(created_date)) as created_date,
    parseDateTime64BestEffortOrNull(toString(modified_date)) as modified_date,
    parseDateTime64BestEffortOrNull(toString(last_activity)) as last_activity,
    
    -- Batch metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'orion.finance_account_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM orion.finance_account_raw
WHERE finance_account_id IS NOT NULL
  AND finance_account_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY finance_account_id, glynac_organization_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1