SELECT
    deal_id,
    glynac_organization_id,
    
    -- Deal classification
    trimBoth(COALESCE(deal_type, '')) as deal_type,
    trimBoth(COALESCE(deal_name, '')) as deal_name,
    trimBoth(COALESCE(stage, '')) as stage,
    trimBoth(COALESCE(deal_status, '')) as deal_status,
    
    -- Financial details (PII)
    toDecimal64OrZero(amount, 2) as amount,
    trimBoth(COALESCE(currency, '')) as currency,
    toDecimal64OrZero(probability, 2) as probability,
    
    -- Dates (parse string to DateTime)
    parseDateTime64BestEffortOrNull(toString(expected_close_date)) as expected_close_date,
    parseDateTime64BestEffortOrNull(toString(actual_close_date)) as actual_close_date,
    parseDateTime64BestEffortOrNull(toString(created_date)) as created_date,
    parseDateTime64BestEffortOrNull(toString(modified_date)) as modified_date,
    
    -- Relationships
    trimBoth(COALESCE(customer_id, '')) as customer_id,
    trimBoth(COALESCE(opportunity_id, '')) as opportunity_id,
    trimBoth(COALESCE(contact_id, '')) as contact_id,
    trimBoth(COALESCE(owner_id, '')) as owner_id,
    
    -- Deal details (PII)
    trimBoth(COALESCE(description, '')) as description,
    trimBoth(COALESCE(terms, '')) as terms,
    trimBoth(COALESCE(products, '')) as products,
    
    -- Batch metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'orion.deal_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM orion.deal_raw
WHERE deal_id IS NOT NULL
  AND deal_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY deal_id, glynac_organization_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1