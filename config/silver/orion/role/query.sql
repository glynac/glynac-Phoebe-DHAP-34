SELECT
    role_id,
    glynac_organization_id,
    
    -- Role classification
    trimBoth(COALESCE(role_type, '')) as role_type,
    trimBoth(COALESCE(role_status, '')) as role_status,
    if(is_system_role = true, 1, 0) as is_system_role,
    
    -- Role details
    trimBoth(COALESCE(role_name, '')) as role_name,
    trimBoth(COALESCE(display_name, '')) as display_name,
    trimBoth(COALESCE(description, '')) as description,
    
    -- Relationships
    trimBoth(COALESCE(organization_id, '')) as organization_id,
    
    -- Permissions
    trimBoth(COALESCE(permissions, '')) as permissions,
    
    -- Dates
    parseDateTime64BestEffortOrNull(toString(created_date)) as created_date,
    parseDateTime64BestEffortOrNull(toString(modified_date)) as modified_date,
    
    -- Batch metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'orion.role_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM orion.role_raw
WHERE role_id IS NOT NULL
  AND role_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY role_id, glynac_organization_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1