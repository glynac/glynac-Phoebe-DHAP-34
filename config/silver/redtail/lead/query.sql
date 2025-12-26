SELECT
    rec_id,
    glynac_organization_id,
    
    -- Lead identifiers
    lead_id,
    
    -- Name fields
    trimBoth(COALESCE(first_name, '')) as first_name,
    trimBoth(COALESCE(last_name, '')) as last_name,
    trimBoth(COALESCE(full_name, '')) as full_name,
    
    -- Contact information
    trimBoth(COALESCE(email, '')) as email,
    trimBoth(COALESCE(phone_number, '')) as phone_number,
    
    -- Professional information
    trimBoth(COALESCE(company_name, '')) as company_name,
    trimBoth(COALESCE(job_title, '')) as job_title,
    trimBoth(COALESCE(industry, '')) as industry,
    
    -- Lead management
    trimBoth(COALESCE(lead_status, '')) as lead_status,
    
    -- Status normalization
    multiIf(
        lead_status ILIKE '%new%', 'new',
        lead_status ILIKE '%contacted%', 'contacted',
        lead_status ILIKE '%qualified%', 'qualified',
        lead_status ILIKE '%unqualified%', 'unqualified',
        lead_status ILIKE '%converted%', 'converted',
        lead_status ILIKE '%closed%', 'closed',
        lead_status != '', Lower(trimBoth(lead_status)),
        'new'
    ) as status_normalized,
    
    trimBoth(COALESCE(source, '')) as source,
    trimBoth(COALESCE(rating, '')) as rating,
    
    -- Rating normalization
    multiIf(
        rating ILIKE '%hot%', 'hot',
        rating ILIKE '%warm%', 'warm',
        rating ILIKE '%cold%', 'cold',
        rating != '', Lower(trimBoth(rating)),
        'unknown'
    ) as rating_normalized,
    
    assigned_to,
    
    -- Conversion tracking
    converted_to_prospect,
    parseDateTime64BestEffortOrNull(toString(converted_date)) as converted_date,
    
    -- Notes
    trimBoth(COALESCE(notes, '')) as notes,
    
    -- Audit fields
    parseDateTime64BestEffortOrNull(toString(rec_add)) as rec_add,
    parseDateTime64BestEffortOrNull(toString(rec_edit)) as rec_edit,
    rec_add_user,
    rec_edit_user,
    
    -- Batch metadata
    trimBoth(COALESCE(scan_id, '')) as scan_id,
    batch_number,
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'redtail.lead_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM redtail.lead_raw
WHERE rec_id IS NOT NULL
  AND rec_id != 0
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rec_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC, batch_number DESC 
) = 1