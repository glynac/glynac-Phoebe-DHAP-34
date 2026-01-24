SELECT
    lead_id,
    glynac_organization_id,
    
    -- Lead classification
    trimBoth(COALESCE(lead_type, '')) as lead_type,
    trimBoth(COALESCE(lead_status, '')) as lead_status,
    trimBoth(COALESCE(lead_source, '')) as lead_source,
    toInt32OrZero(lead_score) as lead_score,
    
    -- Personal information (PII)
    trimBoth(COALESCE(first_name, '')) as first_name,
    trimBoth(COALESCE(last_name, '')) as last_name,
    trimBoth(COALESCE(email, '')) as email,
    trimBoth(COALESCE(phone, '')) as phone,
    
    -- Company information (PII)
    trimBoth(COALESCE(company, '')) as company,
    trimBoth(COALESCE(job_title, '')) as job_title,
    trimBoth(COALESCE(industry, '')) as industry,
    
    -- Lead management
    trimBoth(COALESCE(owner_id, '')) as owner_id,
    toDecimal64OrZero(estimated_value, 2) as estimated_value,
    
    -- Dates
    parseDateTime64BestEffortOrNull(toString(created_date)) as created_date,
    parseDateTime64BestEffortOrNull(toString(modified_date)) as modified_date,
    parseDateTime64BestEffortOrNull(toString(last_activity_date)) as last_activity_date,
    
    -- Batch metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'orion.lead_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM orion.lead_raw
WHERE lead_id IS NOT NULL
  AND lead_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY lead_id, glynac_organization_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1