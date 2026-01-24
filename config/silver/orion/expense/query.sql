SELECT
    expense_id,
    glynac_organization_id,
    
    -- Expense classification
    trimBoth(COALESCE(expense_type, '')) as expense_type,
    trimBoth(COALESCE(category, '')) as category,
    trimBoth(COALESCE(expense_status, '')) as expense_status,
    
    -- Reference and description
    trimBoth(COALESCE(reference, '')) as reference,
    trimBoth(COALESCE(description, '')) as description,
    
    -- Financial details
    toDecimal64OrZero(amount, 2) as amount,
    trimBoth(COALESCE(currency, '')) as currency,
    toDecimal64OrZero(tax, 2) as tax,
    
    -- Relationships
    trimBoth(COALESCE(account_id, '')) as account_id,
    trimBoth(COALESCE(organization_id, '')) as organization_id,
    trimBoth(COALESCE(budget_id, '')) as budget_id,
    trimBoth(COALESCE(submitted_by_id, '')) as submitted_by_id,
    trimBoth(COALESCE(approved_by_id, '')) as approved_by_id,
    trimBoth(COALESCE(customer_id, '')) as customer_id,
    
    -- Flags
    if(reimbursable = true, 1, 0) as reimbursable,
    if(reimbursed = true, 1, 0) as reimbursed,
    if(billable = true, 1, 0) as billable,
    if(billed = true, 1, 0) as billed,
    
    -- Vendor and payment
    trimBoth(COALESCE(vendor, '')) as vendor,
    trimBoth(COALESCE(payment_method, '')) as payment_method,
    trimBoth(COALESCE(receipt_url, '')) as receipt_url,
    
    -- Additional information
    trimBoth(COALESCE(notes, '')) as notes,
    trimBoth(COALESCE(tags, '')) as tags,
    
    -- Dates
    parseDateTime64BestEffortOrNull(toString(expense_date)) as expense_date,
    parseDateTime64BestEffortOrNull(toString(submission_date)) as submission_date,
    parseDateTime64BestEffortOrNull(toString(approval_date)) as approval_date,
    parseDateTime64BestEffortOrNull(toString(created_date)) as created_date,
    parseDateTime64BestEffortOrNull(toString(modified_date)) as modified_date,
    
    -- Batch metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'orion.expense_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM orion.expense_raw
WHERE expense_id IS NOT NULL
  AND expense_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY expense_id, glynac_organization_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1