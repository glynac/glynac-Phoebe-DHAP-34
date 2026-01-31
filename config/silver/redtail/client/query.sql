SELECT
    rec_id,
    glynac_organization_id,
    
    -- Client identifiers
    client_id,
    contact_id,
    trim(COALESCE(client_number, '')) as client_number,
    
    -- Client classification
    trim(COALESCE(client_status, '')) as client_status,
    
    -- Status normalization
    multiIf(
        client_status ILIKE '%active%', 'active',
        client_status ILIKE '%inactive%', 'inactive',
        client_status ILIKE '%archived%', 'archived',
        client_status ILIKE '%prospect%', 'prospect',
        client_status != '', lower(trim(client_status)),
        'active'
    ) as status_normalized,
    
    -- Service details
    trim(COALESCE(service_level, '')) as service_level,
    trim(COALESCE(risk_tolerance, '')) as risk_tolerance,
    trim(COALESCE(investment_objective, '')) as investment_objective,
    
    -- Financial data (already Float64 in Bronze)
    COALESCE(aum, 0.0) as aum,
    COALESCE(annual_revenue, 0.0) as annual_revenue,
    
    -- Important dates
    parseDateTime64BestEffortOrNull(toString(client_since)) as client_since,
    parseDateTime64BestEffortOrNull(toString(next_review_date)) as next_review_date,
    
    -- Notes
    trim(COALESCE(notes, '')) as notes,
    
    -- Status flag
    COALESCE(is_active, true) as is_active,
    
    -- Audit fields
    parseDateTime64BestEffortOrNull(toString(rec_add)) as rec_add,
    parseDateTime64BestEffortOrNull(toString(rec_edit)) as rec_edit,
    rec_add_user,
    rec_edit_user,
    
    -- Batch metadata
    trim(COALESCE(scan_id, '')) as scan_id,
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