SELECT
    rec_id,
    glynac_organization_id,
    
    -- Email identifiers
    email_id,
    
    -- Email addresses
    trimBoth(COALESCE(from_address, '')) as from_address,
    trimBoth(COALESCE(to_addresses, '')) as to_addresses,
    trimBoth(COALESCE(cc_addresses, '')) as cc_addresses,
    trimBoth(COALESCE(bcc_addresses, '')) as bcc_addresses,
    
    -- Email content
    trimBoth(COALESCE(subject, '')) as subject,
    trimBoth(COALESCE(body_text, '')) as body_text,
    trimBoth(COALESCE(body_html, '')) as body_html,
    
    -- Relationships
    contact_id,
    sent_by,
    
    -- Status
    trimBoth(COALESCE(email_status, '')) as email_status,
    
    -- Status normalization
    multiIf(
        email_status ILIKE '%sent%', 'sent',
        email_status ILIKE '%delivered%', 'delivered',
        email_status ILIKE '%bounced%', 'bounced',
        email_status ILIKE '%failed%', 'failed',
        email_status ILIKE '%draft%', 'draft',
        email_status ILIKE '%queued%', 'queued',
        email_status != '', Lower(trimBoth(email_status)),
        'unknown'
    ) as status_normalized,
    
    is_read,
    
    -- Email timing
    parseDateTime64BestEffortOrNull(toString(sent_date)) as sent_date,
    parseDateTime64BestEffortOrNull(toString(read_date)) as read_date,
    parseDateTime64BestEffortOrNull(toString(delivered_date)) as delivered_date,
    
    -- Engagement metrics
    open_count,
    click_count,
    
    -- Attachments
    has_attachments,
    attachments_count,
    
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
    'redtail.email_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM redtail.email_raw
WHERE rec_id IS NOT NULL
  AND rec_id != 0
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rec_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC, batch_number DESC 
) = 1