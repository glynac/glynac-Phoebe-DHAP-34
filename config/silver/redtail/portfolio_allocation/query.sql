SELECT
    rec_id,
    glynac_organization_id,
    
    -- Identifiers
    record_id,
    portfolio_id,
    investment_id,
    
    -- Allocation percentages (convert to decimal, handle % symbol)
    toDecimal64OrZero(replaceAll(replaceAll(target_percentage, '%', ''), ',', ''), 4) as target_percentage,
    toDecimal64OrZero(replaceAll(replaceAll(current_percentage, '%', ''), ',', ''), 4) as current_percentage,
    toDecimal64OrZero(replaceAll(replaceAll(min_percentage, '%', ''), ',', ''), 4) as min_percentage,
    toDecimal64OrZero(replaceAll(replaceAll(max_percentage, '%', ''), ',', ''), 4) as max_percentage,
    
    -- Additional data
    trimBoth(COALESCE(investment_data, '')) as investment_data,
    trimBoth(COALESCE(notes, '')) as notes,
    
    -- Flags
    is_core_holding,
    is_active,
    
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
    'redtail.portfolio_allocation_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM redtail.portfolio_allocation_raw
WHERE rec_id IS NOT NULL
  AND rec_id != 0
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rec_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC, batch_number DESC 
) = 1