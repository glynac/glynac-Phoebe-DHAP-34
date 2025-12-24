SELECT
    rec_id,
    glynac_organization_id,
    
    -- Communication identifiers
    communication_id,
    
    -- Communication details
    trimBoth(COALESCE(communication_type, '')) as communication_type,
    trimBoth(COALESCE(direction, '')) as direction,
    trimBoth(COALESCE(subject, '')) as subject,
    trimBoth(COALESCE(body, '')) as body,
    
    -- Relationships
    contact_id,
    from_user_id,
    to_contact_id,
    
    -- Status
    trimBoth(COALESCE(comm_status, '')) as comm_status,
    
    -- Status normalization
    multiIf(
        comm_status ILIKE '%sent%', 'sent',
        comm_status ILIKE '%delivered%', 'delivered',
        comm_status ILIKE '%read%', 'read',
        comm_status ILIKE '%failed%', 'failed',
        comm_status ILIKE '%draft%', 'draft',
        comm_status != '', toLower(trimBoth(comm_status)),
        'unknown'
    ) as status_normalized,
    
    read_status,
    
    -- Communication timing
    parseDateTime64BestEffortOrNull(toString(communication_date)) as communication_date,
    
    -- Attachments
    attachments_count,
    
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
    'redtail.communication_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM redtail.communication_raw
WHERE rec_id IS NOT NULL
  AND rec_id != 0
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rec_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC, batch_number DESC 
) = 1