SELECT
    -- Mapping keys
    trimBoth(COALESCE(relationship_id, '')) as relationship_id,
    CAST(firm_user_id as Int64) as firm_user_id,
    
    -- Flag
    COALESCE(is_primary, false) as is_primary,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.relationship_advisors_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.relationship_advisors_raw
WHERE relationship_id IS NOT NULL
  AND relationship_id != ''
  AND firm_user_id IS NOT NULL
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, relationship_id, firm_user_id, processing_date 
    ORDER BY is_primary DESC
) = 1