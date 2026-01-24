SELECT
    customer_id,
    glynac_organization_id,
    
    -- Customer classification
    trimBoth(COALESCE(customer_type, '')) as customer_type,
    trimBoth(COALESCE(customer_segment, '')) as customer_segment,
    trimBoth(COALESCE(customer_status, '')) as customer_status,
    
    -- Company information (PII)
    trimBoth(COALESCE(company_name, '')) as company_name,
    trimBoth(COALESCE(industry, '')) as industry,
    trimBoth(COALESCE(company_size, '')) as company_size,
    toInt64OrZero(annual_revenue) as annual_revenue,
    
    -- Contact information (PII)
    trimBoth(COALESCE(website, '')) as website,
    trimBoth(COALESCE(phone, '')) as phone,
    trimBoth(COALESCE(email, '')) as email,
    
    -- Relationship management
    parseDateTime64BestEffortOrNull(toString(customer_since)) as customer_since,
    trimBoth(COALESCE(account_manager_id, '')) as account_manager_id,
    trimBoth(COALESCE(source, '')) as source,
    
    -- Dates (parse string to DateTime)
    parseDateTime64BestEffortOrNull(toString(created_date)) as created_date,
    parseDateTime64BestEffortOrNull(toString(modified_date)) as modified_date,
    
    -- Batch metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'orion.customer_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM orion.customer_raw
WHERE customer_id IS NOT NULL
  AND customer_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id, glynac_organization_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1