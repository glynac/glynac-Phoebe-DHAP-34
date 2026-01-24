SELECT
    contact_id,
    glynac_organization_id,
    
    -- Contact type
    trimBoth(COALESCE(contact_type, '')) as contact_type,
    
    -- Personal information (PII)
    trimBoth(COALESCE(first_name, '')) as first_name,
    trimBoth(COALESCE(last_name, '')) as last_name,
    trimBoth(COALESCE(email, '')) as email,
    trimBoth(COALESCE(phone, '')) as phone,
    trimBoth(COALESCE(mobile, '')) as mobile,
    
    -- Professional information
    trimBoth(COALESCE(job_title, '')) as job_title,
    trimBoth(COALESCE(department, '')) as department,
    
    -- Relationships
    trimBoth(COALESCE(customer_id, '')) as customer_id,
    trimBoth(COALESCE(owner_id, '')) as owner_id,
    
    -- Status and source
    trimBoth(COALESCE(contact_status, '')) as contact_status,
    trimBoth(COALESCE(lead_source, '')) as lead_source,
    
    -- Flags (convert Bool to UInt8)
    if(is_primary_contact = true, 1, 0) as is_primary_contact,
    
    -- Address (PII)
    trimBoth(COALESCE(address, '')) as address,
    
    -- Dates (parse string to DateTime)
    parseDateTime64BestEffortOrNull(toString(created_date)) as created_date,
    parseDateTime64BestEffortOrNull(toString(modified_date)) as modified_date,
    
    -- Batch metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'orion.contact_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM orion.contact_raw
WHERE contact_id IS NOT NULL
  AND contact_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY contact_id, glynac_organization_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1