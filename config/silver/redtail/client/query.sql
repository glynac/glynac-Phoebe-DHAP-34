SELECT
    rec_id,
    glynac_organization_id,
    
    -- Client identifiers
    client_id,
    contact_id,
    trimBoth(COALESCE(client_number, '')) as client_number,
    
    -- Client classification
    trimBoth(COALESCE(client_type, '')) as client_type,
    trimBoth(COALESCE(client_status, '')) as client_status,
    
    -- Status normalization
    multiIf(
        client_status ILIKE '%active%', 'active',
        client_status ILIKE '%inactive%', 'inactive',
        client_status ILIKE '%archived%', 'archived',
        client_status ILIKE '%prospect%', 'prospect',
        client_status != '', toLower(trimBoth(client_status)),
        'active'
    ) as status_normalized,
    
    -- Service details
    trimBoth(COALESCE(service_level, '')) as service_level,
    trimBoth(COALESCE(risk_profile, '')) as risk_profile,
    
    -- Financial data
    toDecimal64OrZero(replaceAll(replaceAll(assets_under_management, ',', ''), '$', ''), 2) as assets_under_management,
    
    -- Important dates
    parseDateTime64BestEffortOrNull(toString(onboarding_date)) as onboarding_date,
    parseDateTime64BestEffortOrNull(toString(review_date)) as review_date,
    
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
    'redtail.client_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM redtail.client_raw
WHERE rec_id IS NOT NULL
  AND rec_id != 0
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rec_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC, batch_number DESC 
) = 1