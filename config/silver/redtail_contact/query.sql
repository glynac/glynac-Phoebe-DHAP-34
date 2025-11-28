SELECT
    rec_id,
    glynac_organization_id,
    
    -- Name fields
    trimBoth(COALESCE(first_name, '')) as first_name,
    trimBoth(COALESCE(last_name, '')) as last_name,
    trimBoth(COALESCE(full_name, '')) as full_name,
    trimBoth(COALESCE(prefix, '')) as salutation,
    
    -- Contact information
    trimBoth(COALESCE(email, '')) as email,
    
    -- Phone numbers
    trimBoth(COALESCE(phone_number, '')) as phone,
    trimBoth(COALESCE(phone_number2, '')) as phone_2,
    trimBoth(COALESCE(mobile_phone, '')) as mobile,
    trimBoth(COALESCE(work_phone, '')) as work_phone,
    trimBoth(COALESCE(home_phone, '')) as home_phone,
    trimBoth(COALESCE(fax_number, '')) as fax,
    
    -- Address information
    trimBoth(COALESCE(address1, '')) as address_line1,
    trimBoth(COALESCE(city, '')) as city,
    trimBoth(COALESCE(state, '')) as state,
    trimBoth(COALESCE(zip_code, '')) as zip_code,
    trimBoth(COALESCE(country, 'USA')) as country,
    
    -- Personal information
    parseDateTime64BestEffortOrNull(toString(date_of_birth)) as date_of_birth,
    trimBoth(COALESCE(gender, '')) as gender,
    trimBoth(COALESCE(marital_status, '')) as marital_status,
    
    -- Professional information
    trimBoth(COALESCE(company_name, '')) as company,
    trimBoth(COALESCE(job_title, '')) as job_title,
    trimBoth(COALESCE(occupation, '')) as occupation,
    
    -- Contact classification
    trimBoth(COALESCE(contact_type, 'Contact')) as contact_type,
    trimBoth(COALESCE(contact_status, 'Active')) as status,
    
    -- Status normalization
    multiIf(
        contact_status ILIKE '%active%', 'active',
        contact_status ILIKE '%inactive%', 'inactive',
        contact_status ILIKE '%archived%', 'archived',
        contact_status != '', toLower(trimBoth(contact_status)),
        'active'
    ) as status_normalized,
    
    -- Foreign key (assigned_to = owner_id in model)
    assigned_to as owner_id,
    
    -- Audit fields
    parseDateTime64BestEffortOrNull(toString(rec_add)) as rec_add,
    parseDateTime64BestEffortOrNull(toString(rec_edit)) as rec_edit,
    
    -- Batch metadata
    trimBoth(COALESCE(scan_id, '')) as scan_id,
    batch_number,
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'redtail.contact_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM redtail.contact_raw
WHERE rec_id IS NOT NULL
  AND rec_id != 0
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rec_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC, batch_number DESC 
) = 1