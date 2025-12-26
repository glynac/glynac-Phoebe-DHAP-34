SELECT
    rec_id,
    glynac_organization_id,
    
    -- Family account identifiers
    rec_id as family_account_id,
    trimBoth(COALESCE(family_name, '')) as family_name,
    trimBoth(COALESCE(account_number, '')) as account_number,
    
    -- Account status
    trimBoth(COALESCE(account_status, '')) as account_status,
    
    -- Status normalization
    multiIf(
        account_status ILIKE '%active%', 'active',
        account_status ILIKE '%inactive%', 'inactive',
        account_status ILIKE '%closed%', 'closed',
        account_status ILIKE '%suspended%', 'suspended',
        account_status != '', toLower(trimBoth(account_status)),
        'active'
    ) as status_normalized,
    
    -- Financial data
    toDecimal64OrZero(replaceAll(replaceAll(total_value, ',', ''), '$', ''), 2) as total_value,
    
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
    'redtail.family_account_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM redtail.family_account_raw
WHERE rec_id IS NOT NULL
  AND rec_id != 0
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rec_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC, batch_number DESC 
) = 1