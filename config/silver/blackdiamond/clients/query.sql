SELECT
    -- Client identifiers
    user_id,
    trimBoth(COALESCE(username, ''))           AS username,
    trimBoth(COALESCE(email, ''))              AS email,
    trimBoth(COALESCE(external_contact_id, '')) AS external_contact_id,

    -- Personal information
    trimBoth(COALESCE(first_name, ''))         AS first_name,
    trimBoth(COALESCE(last_name, ''))          AS last_name,

    -- Status and access
    trimBoth(COALESCE(status, ''))             AS status,
    COALESCE(primary_contact, false)           AS primary_contact,  -- Bool, not String

    -- Profile information
    profile_name,
    trimBoth(COALESCE(profile_v2_name, ''))    AS profile_v2_name,

    -- Activity metrics
    accounts_assigned,
    logins_count,

    -- Organization and partition keys
    glynac_organization_id,
    processing_date,

    -- System columns
    now()                                      AS _loaded_at,
    'blackdiamond.clients_raw'                 AS _source_table,
    processing_date                            AS _source_timestamp

FROM blackdiamond.clients_raw
WHERE user_id IS NOT NULL
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, user_id
    ORDER BY processing_date DESC
) = 1;