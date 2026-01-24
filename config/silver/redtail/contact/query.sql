SELECT
    rec_id,
    glynac_organization_id,

    -- Name fields (RENAMED: PII token aliasing policy)
    trimBoth(COALESCE(first_name, '')) as first_name_hash,
    trimBoth(COALESCE(last_name, '')) as last_name_hash,
    trimBoth(COALESCE(full_name, '')) as full_name_hash,
    trimBoth(COALESCE(prefix, '')) as salutation,

    -- Contact information (RENAMED: PII token aliasing policy)
    trimBoth(COALESCE(email, '')) as email_hash,

    -- Phone numbers (RENAMED: PII token aliasing policy)
    trimBoth(COALESCE(phone_number, '')) as phone_hash,
    trimBoth(COALESCE(phone_number2, '')) as phone_2_hash,
    trimBoth(COALESCE(mobile_phone, '')) as mobile_hash,
    trimBoth(COALESCE(work_phone, '')) as work_phone_hash,
    trimBoth(COALESCE(home_phone, '')) as home_phone_hash,
    trimBoth(COALESCE(fax_number, '')) as fax_hash,

    -- Address information (RENAMED: PII token aliasing policy)
    trimBoth(COALESCE(address1, '')) as address_hash,
    trimBoth(COALESCE(city, '')) as city_hash,
    trimBoth(COALESCE(state, '')) as state,
    trimBoth(COALESCE(zip_code, '')) as zip_hash,
    trimBoth(COALESCE(country, 'USA')) as country,

    -- Personal information (RENAMED: PII token aliasing policy)
    parseDateTime64BestEffortOrNull(toString(date_of_birth)) as dob_hash,
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
