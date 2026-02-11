SELECT
    -- Contact identifier
    trimBoth(COALESCE(id, '')) as id,
    
    -- Name components
    trimBoth(COALESCE(salutation, '')) as salutation,
    trimBoth(COALESCE(first_name, '')) as first_name,
    trimBoth(COALESCE(middle_name, '')) as middle_name,
    trimBoth(COALESCE(last_name, '')) as last_name,
    trimBoth(COALESCE(suffix, '')) as suffix,
    trimBoth(COALESCE(nickname, '')) as nickname,
    
    -- User and member information
    trimBoth(COALESCE(user_id, '')) as user_id,
    member_type_id,
    member_type,
    trimBoth(COALESCE(member_role, '')) as member_role,
    team,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.contacts_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.contacts_raw
WHERE id IS NOT NULL
  AND id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, id, processing_date 
    ORDER BY COALESCE(last_name, '') DESC, COALESCE(first_name, '') DESC
) = 1