SELECT
    note_rec_id as rec_id,
    glynac_organization_id,
    
    -- Note identifiers
    note_id,
    
    -- Note content
    trimBoth(COALESCE(title, '')) as title,
    trimBoth(COALESCE(content, '')) as content,
    
    -- Note classification
    trimBoth(COALESCE(note_type, '')) as note_type,
    trimBoth(COALESCE(category, '')) as category,
    trimBoth(COALESCE(tags, '')) as tags,
    
    -- Relationships
    contact_id,
    created_by,
    
    -- Note properties
    is_private,
    is_pinned,
    
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
    'redtail.note_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM redtail.note_raw
WHERE note_rec_id IS NOT NULL
  AND note_rec_id != 0
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY note_rec_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC, batch_number DESC 
) = 1