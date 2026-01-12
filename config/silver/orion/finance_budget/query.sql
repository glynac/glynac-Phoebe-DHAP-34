SELECT
    budget_id,
    glynac_organization_id,
    
    -- Budget classification
    trimBoth(COALESCE(budget_type, '')) as budget_type,
    trimBoth(COALESCE(name, '')) as name,
    trimBoth(COALESCE(category, '')) as category,
    trimBoth(COALESCE(budget_status, '')) as budget_status,
    trimBoth(COALESCE(description, '')) as description,
    
    -- Relationships
    trimBoth(COALESCE(account_id, '')) as account_id,
    trimBoth(COALESCE(organization_id, '')) as organization_id,
    trimBoth(COALESCE(created_by_id, '')) as created_by_id,
    trimBoth(COALESCE(approved_by_id, '')) as approved_by_id,
    trimBoth(COALESCE(parent_budget_id, '')) as parent_budget_id,
    
    -- Period information
    trimBoth(COALESCE(period, '')) as period,
    parseDateTime64BestEffortOrNull(toString(start_date)) as start_date,
    parseDateTime64BestEffortOrNull(toString(end_date)) as end_date,
    
    -- Financial details
    toDecimal64OrZero(amount, 2) as amount,
    trimBoth(COALESCE(currency, '')) as currency,
    toDecimal64OrZero(allocated, 2) as allocated,
    toDecimal64OrZero(spent, 2) as spent,
    toDecimal64OrZero(remaining, 2) as remaining,
    trimBoth(COALESCE(allocation, '')) as allocation,
    
    -- Additional information
    trimBoth(COALESCE(notes, '')) as notes,
    trimBoth(COALESCE(tags, '')) as tags,
    
    -- Dates
    parseDateTime64BestEffortOrNull(toString(created_date)) as created_date,
    parseDateTime64BestEffortOrNull(toString(modified_date)) as modified_date,
    parseDateTime64BestEffortOrNull(toString(last_review_date)) as last_review_date,
    
    -- Batch metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'orion.finance_budget_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM orion.finance_budget_raw
WHERE budget_id IS NOT NULL
  AND budget_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY budget_id, glynac_organization_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1