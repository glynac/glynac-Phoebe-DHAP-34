SELECT
    revenue_id,
    glynac_organization_id,
    
    -- Revenue classification
    trimBoth(COALESCE(revenue_type, '')) as revenue_type,
    trimBoth(COALESCE(category, '')) as category,
    trimBoth(COALESCE(revenue_status, '')) as revenue_status,
    
    -- Reference and description
    trimBoth(COALESCE(reference, '')) as reference,
    trimBoth(COALESCE(description, '')) as description,
    
    -- Financial details
    toDecimal64OrZero(amount, 2) as amount,
    trimBoth(COALESCE(currency, '')) as currency,
    toDecimal64OrZero(tax, 2) as tax,
    toDecimal64OrZero(cost_of_sales, 2) as cost_of_sales,
    toDecimal64OrZero(margin, 2) as margin,
    
    -- Relationships
    trimBoth(COALESCE(account_id, '')) as account_id,
    trimBoth(COALESCE(organization_id, '')) as organization_id,
    trimBoth(COALESCE(finance_account_id, '')) as finance_account_id,
    trimBoth(COALESCE(customer_id, '')) as customer_id,
    trimBoth(COALESCE(invoice_id, '')) as invoice_id,
    trimBoth(COALESCE(payment_id, '')) as payment_id,
    trimBoth(COALESCE(deal_id, '')) as deal_id,
    trimBoth(COALESCE(created_by_id, '')) as created_by_id,
    trimBoth(COALESCE(approved_by_id, '')) as approved_by_id,
    
    -- Recognition details
    trimBoth(COALESCE(recognition_model, '')) as recognition_model,
    parseDateTime64BestEffortOrNull(toString(recognition_date)) as recognition_date,
    
    -- Additional information
    trimBoth(COALESCE(notes, '')) as notes,
    trimBoth(COALESCE(tags, '')) as tags,
    
    -- Dates
    parseDateTime64BestEffortOrNull(toString(created_date)) as created_date,
    parseDateTime64BestEffortOrNull(toString(modified_date)) as modified_date,
    
    -- Batch metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'orion.finance_revenue_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM orion.finance_revenue_raw
WHERE revenue_id IS NOT NULL
  AND revenue_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY revenue_id, glynac_organization_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1