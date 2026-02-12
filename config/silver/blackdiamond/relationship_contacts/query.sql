SELECT
    -- Mapping keys
    trimBoth(COALESCE(relationship_id, '')) as relationship_id,
    trimBoth(COALESCE(contact_id, '')) as contact_id,
    
    -- Role flags
    COALESCE(is_primary, false) as is_primary,
    COALESCE(is_secondary, false) as is_secondary,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.relationship_contacts_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.relationship_contacts_raw
WHERE relationship_id IS NOT NULL
  AND relationship_id != ''
  AND contact_id IS NOT NULL
  AND contact_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
  -- Exclude invalid combinations where both flags are true
  AND NOT (is_primary = true AND is_secondary = true)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, relationship_id, contact_id, processing_date 
    ORDER BY is_primary DESC, is_secondary DESC
) = 1