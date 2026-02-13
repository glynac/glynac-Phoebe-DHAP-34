SELECT
    -- Primary identifiers
    CAST(contact_id as Int64) as contact_id,
    rec_id,
    
    -- Personal information
    trimBoth(COALESCE(first_name, '')) as first_name,
    trimBoth(COALESCE(last_name, '')) as last_name,
    trimBoth(COALESCE(full_name, '')) as full_name,
    trimBoth(COALESCE(prefix, '')) as prefix,
    
    -- Contact methods
    trimBoth(COALESCE(email, '')) as email,
    trimBoth(COALESCE(phone_number, '')) as phone_number,
    trimBoth(COALESCE(phone_number2, '')) as phone_number2,
    trimBoth(COALESCE(mobile_phone, '')) as mobile_phone,
    trimBoth(COALESCE(work_phone, '')) as work_phone,
    trimBoth(COALESCE(home_phone, '')) as home_phone,
    trimBoth(COALESCE(fax_number, '')) as fax_number,
    
    -- Address
    trimBoth(COALESCE(address1, '')) as address1,
    trimBoth(COALESCE(city, '')) as city,
    trimBoth(COALESCE(state, '')) as state,
    trimBoth(COALESCE(zip_code, '')) as zip_code,
    trimBoth(COALESCE(country, '')) as country,
    
    -- Demographics
    parseDateTime64BestEffortOrNull(toString(date_of_birth)) as date_of_birth,
    trimBoth(COALESCE(gender, '')) as gender,
    trimBoth(COALESCE(marital_status, '')) as marital_status,
    
    -- Employment
    trimBoth(COALESCE(occupation, '')) as occupation,
    trimBoth(COALESCE(company_name, '')) as company_name,
    trimBoth(COALESCE(job_title, '')) as job_title,
    
    -- Contact classification
    trimBoth(COALESCE(contact_type, '')) as contact_type,
    trimBoth(COALESCE(contact_status, '')) as contact_status,
    assigned_to,
    
    -- Audit timestamps
    parseDateTime64BestEffortOrNull(toString(rec_add)) as rec_add,
    parseDateTime64BestEffortOrNull(toString(rec_edit)) as rec_edit,
    
    -- Batch metadata
    trimBoth(COALESCE(scan_id, '')) as scan_id,
    parseDateTime64BestEffortOrNull(toString(batch_timestamp)) as batch_timestamp,
    batch_number,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'redtail.contact_raw' as _source_table,
    processing_date as _source_timestamp

FROM redtail.contact_raw
WHERE contact_id IS NOT NULL
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
  AND processing_date IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, contact_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(rec_edit)) DESC NULLS LAST,
             batch_number DESC NULLS LAST
) = 1