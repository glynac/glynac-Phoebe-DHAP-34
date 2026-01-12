SELECT
    account_id,
    glynac_organization_id,
    
    -- Account details
    account_type,
    account_name,
    domain,
    plan,
    account_status,
    
    -- User counts
    toInt32OrZero(max_users) as max_users,
    toInt32OrZero(current_users) as current_users,
    
    -- Company information  
    industry,
    company_size,
    
    -- Contact information (PII)
    trimBoth(COALESCE(email, '')) as email,
    trimBoth(COALESCE(phone, '')) as phone,
    trimBoth(COALESCE(website, '')) as website,
    trimBoth(COALESCE(address, '')) as address,
    
    -- Dates (parse string to DateTime)
    parseDateTime64BestEffortOrNull(toString(created_date)) as created_date,
    parseDateTime64BestEffortOrNull(toString(modified_date)) as modified_date,
    
    -- Settings (JSON blob)
    settings,
    
    -- Batch metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'orion.account_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM orion.account_raw
WHERE account_id IS NOT NULL
  AND account_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY account_id, glynac_organization_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1