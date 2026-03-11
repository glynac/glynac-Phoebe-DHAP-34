SELECT
    -- User identifier (Nullable(Int64) in raw)
    toInt64OrZero(toString(id)) as id,

    -- Name components
    trimBoth(COALESCE(first_name, '')) as first_name,
    trimBoth(COALESCE(last_name, '')) as last_name,

    -- Login credentials
    trimBoth(COALESCE(login, '')) as login,
    trimBoth(COALESCE(email, '')) as email,

    -- Organization and partition keys
    glynac_organization_id,
    processing_date,

    -- System columns
    now() as _loaded_at,
    'blackdiamond.firm_users_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.firm_users_raw
WHERE id IS NOT NULL
  AND glynac_organization_id IS NOT NULL AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, id, processing_date
    ORDER BY COALESCE(email, '') DESC, COALESCE(last_name, '') DESC
) = 1