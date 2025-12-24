SELECT
    rec_id,
    glynac_organization_id,
    
    -- Activity identifiers
    activity_id,
    
    -- Activity classification
    trimBoth(COALESCE(activity_type, '')) as activity_type,
    trimBoth(COALESCE(category, '')) as category,
    trimBoth(COALESCE(subcategory, '')) as subcategory,
    
    -- Activity details
    trimBoth(COALESCE(subject, '')) as subject,
    trimBoth(COALESCE(description, '')) as description,
    
    -- Relationships
    contact_id,
    owner_id,
    
    -- Activity management
    trimBoth(COALESCE(activity_status, '')) as activity_status,
    
    -- Status normalization
    multiIf(
        activity_status ILIKE '%completed%', 'completed',
        activity_status ILIKE '%pending%', 'pending',
        activity_status ILIKE '%in progress%', 'in_progress',
        activity_status ILIKE '%cancelled%', 'cancelled',
        activity_status ILIKE '%scheduled%', 'scheduled',
        activity_status != '', toLower(trimBoth(activity_status)),
        'pending'
    ) as status_normalized,
    
    trimBoth(COALESCE(priority, '')) as priority,
    
    -- Priority normalization
    multiIf(
        priority ILIKE '%high%', 'high',
        priority ILIKE '%medium%', 'medium',
        priority ILIKE '%low%', 'low',
        priority ILIKE '%urgent%', 'urgent',
        priority != '', toLower(trimBoth(priority)),
        'medium'
    ) as priority_normalized,
    
    -- Activity timing
    parseDateTime64BestEffortOrNull(toString(activity_date)) as activity_date,
    duration_minutes,
    
    -- Activity outcome
    trimBoth(COALESCE(outcome, '')) as outcome,
    trimBoth(COALESCE(location, '')) as location,
    trimBoth(COALESCE(tags, '')) as tags,
    
    -- Follow-up
    follow_up_required,
    parseDateTime64BestEffortOrNull(toString(follow_up_date)) as follow_up_date,
    
    -- Notes
    trimBoth(COALESCE(notes, '')) as notes,
    
    -- Audit fields
    parseDateTime64BestEffortOrNull(toString(rec_add)) as rec_add,
    parseDateTime64BestEffortOrNull(toString(rec_edit)) as rec_edit,
    
    -- Batch metadata
    trimBoth(COALESCE(scan_id, '')) as scan_id,
    batch_number,
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'redtail.activity_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM redtail.activity_raw
WHERE rec_id IS NOT NULL
  AND rec_id != 0
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rec_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC, batch_number DESC 
) = 1