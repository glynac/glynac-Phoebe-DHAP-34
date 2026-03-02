-- query.sql
SELECT
    -- Contact and relationship identifiers
    trimBoth(COALESCE(contact_id, '')) as contact_id,
    trimBoth(COALESCE(relationship_id, '')) as relationship_id,
    relationship_internal_id,
    
    -- Member type and role information
    member_type_id,
    member_role_id,
    trimBoth(COALESCE(member_role, '')) as member_role,
    trimBoth(COALESCE(member_role_title, '')) as member_role_title,
    
    -- Relationship flags
    COALESCE(is_primary, false) as is_primary,
    COALESCE(is_secondary, false) as is_secondary,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.contact_relationships_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.contact_relationships_raw
WHERE contact_id IS NOT NULL
  AND contact_id != ''
  AND relationship_id IS NOT NULL
  AND relationship_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, contact_id, relationship_id 
    ORDER BY processing_date DESC
) = 1