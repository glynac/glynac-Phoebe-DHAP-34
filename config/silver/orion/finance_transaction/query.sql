SELECT
    transaction_id,
    glynac_organization_id,
    
    -- Transaction classification
    trimBoth(COALESCE(transaction_type, '')) as transaction_type,
    trimBoth(COALESCE(category, '')) as category,
    trimBoth(COALESCE(transaction_status, '')) as transaction_status,
    
    -- Reference and description
    trimBoth(COALESCE(reference, '')) as reference,
    trimBoth(COALESCE(description, '')) as description,
    
    -- Financial details
    toDecimal64OrZero(amount, 2) as amount,
    trimBoth(COALESCE(currency, '')) as currency,
    
    -- Account relationships
    trimBoth(COALESCE(source_account_id, '')) as source_account_id,
    trimBoth(COALESCE(destination_account_id, '')) as destination_account_id,
    trimBoth(COALESCE(account_id, '')) as account_id,
    trimBoth(COALESCE(organization_id, '')) as organization_id,
    
    -- User relationships
    trimBoth(COALESCE(created_by_id, '')) as created_by_id,
    trimBoth(COALESCE(approved_by_id, '')) as approved_by_id,
    
    -- Associated records
    trimBoth(COALESCE(expense_id, '')) as expense_id,
    trimBoth(COALESCE(invoice_id, '')) as invoice_id,
    trimBoth(COALESCE(payment_id, '')) as payment_id,
    trimBoth(COALESCE(revenue_id, '')) as revenue_id,
    
    -- Additional information
    trimBoth(COALESCE(notes, '')) as notes,
    trimBoth(COALESCE(tags, '')) as tags,
    trimBoth(COALESCE(meta_data, '')) as meta_data,
    
    -- Dates
    parseDateTime64BestEffortOrNull(toString(transaction_date)) as transaction_date,
    parseDateTime64BestEffortOrNull(toString(cleared_date)) as cleared_date,
    parseDateTime64BestEffortOrNull(toString(created_date)) as created_date,
    parseDateTime64BestEffortOrNull(toString(modified_date)) as modified_date,
    
    -- Batch metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'orion.finance_transaction_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM orion.finance_transaction_raw
WHERE transaction_id IS NOT NULL
  AND transaction_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY transaction_id, glynac_organization_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1