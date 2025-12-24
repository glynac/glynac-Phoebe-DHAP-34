SELECT
    rec_id,
    glynac_organization_id,
    
    -- Call identifiers
    call_id,
    
    -- Call details
    trimBoth(COALESCE(call_type, '')) as call_type,
    trimBoth(COALESCE(call_status, '')) as call_status,
    trimBoth(COALESCE(phone_number, '')) as phone_number,
    
    -- Relationships
    contact_id,
    user_id,
    
    -- Call timing
    parseDateTime64BestEffortOrNull(toString(call_date)) as call_date,
    trimBoth(COALESCE(call_time, '')) as call_time,
    duration_seconds,
    duration_minutes,
    
    -- Call content
    trimBoth(COALESCE(subject, '')) as subject,
    trimBoth(COALESCE(notes, '')) as notes,
    trimBoth(COALESCE(disposition, '')) as disposition,
    trimBoth(COALESCE(outcome, '')) as outcome,
    
    -- Follow-up
    follow_up_required,
    parseDateTime64BestEffortOrNull(toString(follow_up_date)) as follow_up_date,
    
    -- Recording
    has_recording,
    trimBoth(COALESCE(recording_url, '')) as recording_url,
    
    -- Audit fields
    parseDateTime64BestEffortOrNull(toString(rec_add)) as rec_add,
    parseDateTime64BestEffortOrNull(toString(rec_edit)) as rec_edit,
    
    -- Batch metadata
    trimBoth(COALESCE(scan_id, '')) as scan_id,
    batch_number,
    toDate(processing_timestamp) as processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'redtail.call_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM redtail.call_raw
WHERE rec_id IS NOT NULL
  AND rec_id != 0
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rec_id, toDate(processing_timestamp)
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC, batch_number DESC 
) = 1