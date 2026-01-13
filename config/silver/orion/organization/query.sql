SELECT
    organization_id,
    glynac_organization_id,
    
    -- Organization classification
    trimBoth(COALESCE(organization_type, '')) as organization_type,
    trimBoth(COALESCE(org_type, '')) as org_type,
    trimBoth(COALESCE(org_status, '')) as org_status,
    
    -- Organization details
    trimBoth(COALESCE(name, '')) as name,
    trimBoth(COALESCE(display_name, '')) as display_name,
    trimBoth(COALESCE(description, '')) as description,
    
    -- Relationships
    trimBoth(COALESCE(account_id, '')) as account_id,
    
    -- Membership and capacity
    toInt32OrZero(member_count) as member_count,
    toInt32OrZero(max_members) as max_members,
    
    -- Financial details
    toInt64OrZero(budget) as budget,
    trimBoth(COALESCE(cost_center, '')) as cost_center,
    
    -- Location and contact
    trimBoth(COALESCE(location, '')) as location,
    trimBoth(COALESCE(email, '')) as email,
    
    -- Settings
    trimBoth(COALESCE(settings, '')) as settings,
    
    -- Dates
    parseDateTime64BestEffortOrNull(toString(created_date)) as created_date,
    parseDateTime64BestEffortOrNull(toString(modified_date)) as modified_date,
    
    -- Batch metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'orion.organization_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM orion.organization_raw
WHERE organization_id IS NOT NULL
  AND organization_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY organization_id, glynac_organization_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1