SELECT
    rec_id,
    glynac_organization_id,
    
    -- Prospect identifiers
    prospect_id,
    contact_id,
    trimBoth(COALESCE(prospect_number, '')) as prospect_number,
    
    -- Prospect management
    trimBoth(COALESCE(prospect_status, '')) as prospect_status,
    
    -- Status normalization
    multiIf(
        prospect_status ILIKE '%qualified%', 'qualified',
        prospect_status ILIKE '%active%', 'active',
        prospect_status ILIKE '%converted%', 'converted',
        prospect_status ILIKE '%closed%', 'closed',
        prospect_status ILIKE '%inactive%', 'inactive',
        prospect_status ILIKE '%lost%', 'lost',
        prospect_status != '', Lower(trimBoth(prospect_status)),
        'active'
    ) as status_normalized,
    
    trimBoth(COALESCE(stage, '')) as stage,
    
    -- Stage normalization
    multiIf(
        stage ILIKE '%discovery%', 'discovery',
        stage ILIKE '%qualification%', 'qualification',
        stage ILIKE '%proposal%', 'proposal',
        stage ILIKE '%negotiation%', 'negotiation',
        stage ILIKE '%closed won%', 'closed_won',
        stage ILIKE '%closed lost%', 'closed_lost',
        stage ILIKE '%contract%', 'contract',
        stage != '', Lower(trimBoth(stage)),
        'discovery'
    ) as stage_normalized,
    
    trimBoth(COALESCE(source, '')) as source,
    
    -- Financial data
    toDecimal64OrZero(replaceAll(replaceAll(expected_revenue, ',', ''), '$', ''), 2) as expected_revenue,
    probability,
    
    -- Important dates
    parseDateTime64BestEffortOrNull(toString(expected_close_date)) as expected_close_date,
    
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
    'redtail.prospect_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM redtail.prospect_raw
WHERE rec_id IS NOT NULL
  AND rec_id != 0
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rec_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC, batch_number DESC 
) = 1