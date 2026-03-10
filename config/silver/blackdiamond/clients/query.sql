SELECT
    -- Client identifiers
    user_id,
    trimBoth(COALESCE(username, '')) as username,
    trimBoth(COALESCE(email, '')) as email,
    trimBoth(COALESCE(external_contact_id, '')) as external_contact_id,
    
    -- Personal information
    trimBoth(COALESCE(first_name, '')) as first_name,
    trimBoth(COALESCE(last_name, '')) as last_name,
    
    -- Status and access
    trimBoth(COALESCE(status, '')) as status,
    trimBoth(COALESCE(primary_contact, '')) as primary_contact,
    
    -- Profile information
    profile_name,
    trimBoth(COALESCE(profile_v2_name, '')) as profile_v2_name,
    
    -- Activity metrics
    accounts_assigned,
    logins_count,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.clients_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.clients_raw
WHERE user_id IS NOT NULL
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, user_id 
    ORDER BY processing_date DESC
) = 1