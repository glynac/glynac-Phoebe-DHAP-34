SELECT
    profile_id,
    glynac_organization_id,
    
    -- Profile classification
    trimBoth(COALESCE(profile_type, '')) as profile_type,
    trimBoth(COALESCE(user_id, '')) as user_id,
    
    -- Contact information (PII)
    trimBoth(COALESCE(phone, '')) as phone,
    trimBoth(COALESCE(address, '')) as address,
    trimBoth(COALESCE(emergency_contact, '')) as emergency_contact,
    
    -- Profile details
    trimBoth(COALESCE(avatar_url, '')) as avatar_url,
    trimBoth(COALESCE(bio, '')) as bio,
    
    -- Preferences
    trimBoth(COALESCE(time_zone, '')) as time_zone,
    trimBoth(COALESCE(language, '')) as language,
    trimBoth(COALESCE(date_format, '')) as date_format,
    trimBoth(COALESCE(notification_preferences, '')) as notification_preferences,
    
    -- Custom data
    trimBoth(COALESCE(custom_fields, '')) as custom_fields,
    
    -- Dates
    parseDateTime64BestEffortOrNull(toString(created_date)) as created_date,
    parseDateTime64BestEffortOrNull(toString(modified_date)) as modified_date,
    
    -- Batch metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'orion.user_profile_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM orion.user_profile_raw
WHERE profile_id IS NOT NULL
  AND profile_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY profile_id, glynac_organization_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1