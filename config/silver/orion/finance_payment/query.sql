SELECT
    payment_id,
    glynac_organization_id,
    
    -- Payment classification
    trimBoth(COALESCE(payment_type, '')) as payment_type,
    trimBoth(COALESCE(payment_status, '')) as payment_status,
    trimBoth(COALESCE(method, '')) as method,
    
    -- Reference and description
    trimBoth(COALESCE(reference, '')) as reference,
    trimBoth(COALESCE(description, '')) as description,
    trimBoth(COALESCE(external_reference, '')) as external_reference,
    
    -- Financial details
    toDecimal64OrZero(amount, 2) as amount,
    trimBoth(COALESCE(currency, '')) as currency,
    toDecimal64OrZero(fees, 2) as fees,
    toDecimal64OrZero(net_amount, 2) as net_amount,
    
    -- Relationships
    trimBoth(COALESCE(account_id, '')) as account_id,
    trimBoth(COALESCE(organization_id, '')) as organization_id,
    trimBoth(COALESCE(finance_account_id, '')) as finance_account_id,
    trimBoth(COALESCE(customer_id, '')) as customer_id,
    trimBoth(COALESCE(invoice_id, '')) as invoice_id,
    trimBoth(COALESCE(created_by_id, '')) as created_by_id,
    trimBoth(COALESCE(verified_by_id, '')) as verified_by_id,
    
    -- Additional information
    trimBoth(COALESCE(notes, '')) as notes,
    trimBoth(COALESCE(meta_data, '')) as meta_data,
    
    -- Dates
    parseDateTime64BestEffortOrNull(toString(payment_date)) as payment_date,
    parseDateTime64BestEffortOrNull(toString(cleared_date)) as cleared_date,
    parseDateTime64BestEffortOrNull(toString(created_date)) as created_date,
    parseDateTime64BestEffortOrNull(toString(modified_date)) as modified_date,
    
    -- Batch metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'orion.finance_payment_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM orion.finance_payment_raw
WHERE payment_id IS NOT NULL
  AND payment_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY payment_id, glynac_organization_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1