SELECT
    invoice_id,
    glynac_organization_id,
    
    -- Invoice classification
    trimBoth(COALESCE(invoice_type, '')) as invoice_type,
    trimBoth(COALESCE(invoice_number, '')) as invoice_number,
    trimBoth(COALESCE(invoice_status, '')) as invoice_status,
    trimBoth(COALESCE(description, '')) as description,
    
    -- Financial details
    toDecimal64OrZero(amount, 2) as amount,
    toDecimal64OrZero(amount_paid, 2) as amount_paid,
    toDecimal64OrZero(amount_due, 2) as amount_due,
    trimBoth(COALESCE(currency, '')) as currency,
    toDecimal64OrZero(tax, 2) as tax,
    
    -- Relationships
    trimBoth(COALESCE(account_id, '')) as account_id,
    trimBoth(COALESCE(organization_id, '')) as organization_id,
    trimBoth(COALESCE(customer_id, '')) as customer_id,
    trimBoth(COALESCE(contact_id, '')) as contact_id,
    trimBoth(COALESCE(deal_id, '')) as deal_id,
    trimBoth(COALESCE(created_by_id, '')) as created_by_id,
    trimBoth(COALESCE(approved_by_id, '')) as approved_by_id,
    
    -- Invoice details
    trimBoth(COALESCE(terms, '')) as terms,
    trimBoth(COALESCE(notes, '')) as notes,
    trimBoth(COALESCE(lineitems, '')) as lineitems,
    trimBoth(COALESCE(meta_data, '')) as meta_data,
    
    -- Dates
    parseDateTime64BestEffortOrNull(toString(issue_date)) as issue_date,
    parseDateTime64BestEffortOrNull(toString(due_date)) as due_date,
    parseDateTime64BestEffortOrNull(toString(paid_date)) as paid_date,
    parseDateTime64BestEffortOrNull(toString(created_date)) as created_date,
    parseDateTime64BestEffortOrNull(toString(modified_date)) as modified_date,
    
    -- Batch metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'orion.finance_invoice_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM orion.finance_invoice_raw
WHERE invoice_id IS NOT NULL
  AND invoice_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY invoice_id, glynac_organization_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1