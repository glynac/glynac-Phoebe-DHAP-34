SELECT
    -- Core user identifier
    trimBoth(COALESCE(user_id, ''))                AS user_id,
    trimBoth(COALESCE(glynac_organization_id, '')) AS glynac_organization_id,

    -- Identity fields
    trimBoth(COALESCE(user_principal_name, ''))    AS user_principal_name,
    trimBoth(COALESCE(display_name, ''))           AS display_name,
    trimBoth(COALESCE(mail, ''))                   AS mail,

    -- Name components
    trimBoth(COALESCE(given_name, ''))             AS given_name,
    trimBoth(COALESCE(surname, ''))                AS surname,

    -- Contact information
    trimBoth(COALESCE(mobile_phone, ''))           AS mobile_phone,

    -- Processing metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) AS processing_timestamp,

    -- System columns
    now()                             AS _loaded_at,
    'microsoft.user_raw'              AS _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) AS _source_timestamp

FROM microsoft.user_raw
WHERE user_id IS NOT NULL
  AND user_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY user_id, glynac_organization_id, processing_date
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1
