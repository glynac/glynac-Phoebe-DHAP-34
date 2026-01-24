SELECT
    user_id,
    glynac_organization_id,

    -- User details
    user_type,
    trimBoth(COALESCE(username, '')) as username,
    trimBoth(COALESCE(email, '')) as email,
    trimBoth(COALESCE(first_name, '')) as first_name,
    trimBoth(COALESCE(last_name, '')) as last_name,
    user_status,
    ifNull(is_active, false) as is_active,
    organization_id,
    role_id,

    -- Dates (parse string to DateTime)
    parseDateTime64BestEffortOrNull(toString(last_login)) as last_login,
    parseDateTime64BestEffortOrNull(toString(created_date)) as created_date,
    parseDateTime64BestEffortOrNull(toString(modified_date)) as modified_date,

    -- Settings (JSON blob)
    settings,

    -- Batch metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,

    -- System columns
    now() as _loaded_at,
    'orion.user_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM orion.user_raw
WHERE user_id IS NOT NULL
  AND user_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY user_id, glynac_organization_id, processing_date
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1
