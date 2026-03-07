SELECT
    -- Contact identifiers
    trimBoth(COALESCE(id, ''))          AS id,
    internal_id,

    -- Name components
    trimBoth(COALESCE(salutation, ''))   AS salutation,
    trimBoth(COALESCE(first_name, ''))   AS first_name,
    trimBoth(COALESCE(middle_name, ''))  AS middle_name,
    trimBoth(COALESCE(last_name, ''))    AS last_name,
    trimBoth(COALESCE(suffix, ''))       AS suffix,
    trimBoth(COALESCE(nickname, ''))     AS nickname,
    trimBoth(COALESCE(display_name, '')) AS display_name,
    trimBoth(COALESCE(full_name, ''))    AS full_name,

    -- Demographic information
    parseDateTime64BestEffortOrNull(toString(date_of_birth))  AS date_of_birth,
    parseDateTime64BestEffortOrNull(toString(date_of_death))  AS date_of_death,
    trimBoth(COALESCE(gender, ''))                            AS gender,
    trimBoth(COALESCE(marital_status, ''))                    AS marital_status,

    -- Financial and retirement information
    COALESCE(toFloat64(income), 0.0)                                        AS income,
    COALESCE(toFloat64(retirement_age), 0.0)                                AS retirement_age,
    parseDateTime64BestEffortOrNull(toString(actual_retirement_date))       AS actual_retirement_date,
    COALESCE(is_disabled, false)                                            AS is_disabled,

    -- Team assignment
    trimBoth(COALESCE(team_id, ''))              AS team_id,
    COALESCE(toFloat64(team_internal_id), 0.0)   AS team_internal_id,
    trimBoth(COALESCE(team_name, ''))            AS team_name,

    -- User association
    COALESCE(toFloat64(user_id), 0.0)   AS user_id,
    user_login,

    -- Organization and partition keys
    glynac_organization_id,
    processing_date,

    -- System columns
    now()                                AS _loaded_at,
    'blackdiamond.contact_profiles_raw'  AS _source_table,
    processing_date                      AS _source_timestamp

FROM blackdiamond.contact_profiles_raw
WHERE id IS NOT NULL
  AND id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, id
    ORDER BY processing_date DESC
) = 1;