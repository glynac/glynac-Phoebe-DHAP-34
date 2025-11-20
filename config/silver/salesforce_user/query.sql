SELECT
    user_id,
    glynac_organization_id,
    trimBoth(COALESCE(username, '')) as username,
    trimBoth(COALESCE(email, '')) as email,
    trimBoth(COALESCE(first_name, '')) as first_name,
    trimBoth(COALESCE(last_name, '')) as last_name,    
    multiIf(
        trimBoth(first_name) != '' AND trimBoth(last_name) != '',
        CONCAT(trimBoth(first_name), ' ', trimBoth(last_name)),
        trimBoth(first_name) != '',
        trimBoth(first_name),
        trimBoth(last_name) != '',
        trimBoth(last_name),
        'Unknown'
    ) as full_name,    
    multiIf(
        email LIKE '%@%',
        substring(email, position(email, '@') + 1),
        ''
    ) as email_domain,
    is_active,
    multiIf(
        is_active = 1, 'active',
        is_active = 0, 'inactive',
        'unknown'
    ) as status,
    trimBoth(COALESCE(profile_id, '')) as profile_id,
    parseDateTime64BestEffortOrNull(toString(created_date)) as created_date,
    parseDateTime64BestEffortOrNull(toString(last_modified_date)) as last_modified_date,
    parseDateTime64BestEffortOrNull(toString(extracted_at)) as extracted_at,    
    dateDiff('day', parseDateTime64BestEffortOrNull(toString(created_date)), now()) as user_age_days,
    trimBoth(COALESCE(source_service, 'salesforce')) as source_service,
    scan_id,
    page_number,
    processing_date,
    now() as _loaded_at,
    'bronze.salesforce.user_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM salesforce.user_raw
WHERE user_id IS NOT NULL
  AND user_id != ''
  AND user_id != 'NONE'
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY user_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC, scan_id DESC 
) = 1