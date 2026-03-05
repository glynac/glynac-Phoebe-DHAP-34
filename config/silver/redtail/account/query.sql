SELECT
    rec_id,
    glynac_organization_id,
    
    -- Account identifiers
    trimBoth(COALESCE(account_number, '')) as account_number,
    trimBoth(COALESCE(account_name, '')) as account_name,
    trimBoth(COALESCE(account_type, '')) as account_type,
    trimBoth(COALESCE(account_status, '')) as account_status,
    
    -- Status flag
    multiIf(
        account_status ILIKE '%active%', 'active',
        account_status ILIKE '%closed%', 'closed',
        account_status ILIKE '%pending%', 'pending',
        account_status != '', lower(trimBoth(account_status)),
        'unknown'
    ) as status_normalized,
    
    -- Description
    trimBoth(COALESCE(description, '')) as description,
    
    -- Institution information
    trimBoth(COALESCE(institution_name, '')) as institution_name,
    trimBoth(COALESCE(institution_type, '')) as institution_type,
    
    -- Foreign keys
    contact_id,
    portfolio_id,
    
    -- JSON data blobs (keep as-is)
    contact_data,
    portfolio_data,
    account_assets_data,
    COALESCE(account_assets_count, 0) as account_assets_count,
    
    -- Financial amounts (convert string to Decimal)
    CAST(
        replaceAll(replaceAll(COALESCE(current_balance, '0'), ',', ''), '$', '') 
        AS Decimal(15, 2)
    ) as current_balance,
    
    CAST(
        replaceAll(replaceAll(COALESCE(available_balance, '0'), ',', ''), '$', '') 
        AS Decimal(15, 2)
    ) as available_balance,
    
    trimBoth(COALESCE(currency, 'USD')) as currency,
    
    -- Dates (parse string to Date/DateTime)
    parseDateTime64BestEffortOrNull(toString(open_date)) as open_date,
    parseDateTime64BestEffortOrNull(toString(rec_add)) as rec_add,
    parseDateTime64BestEffortOrNull(toString(rec_edit)) as rec_edit,
    
    -- Audit fields
    rec_add_user,
    rec_edit_user,
    
    -- Tags and flags
    trimBoth(COALESCE(tags, '')) as tags,
    is_active,
    
    -- Batch metadata
    trimBoth(COALESCE(scan_id, '')) as scan_id,
    batch_number,
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'redtail.account_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM redtail.account_raw
WHERE rec_id IS NOT NULL
  AND rec_id != 0
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rec_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC, batch_number DESC 
) = 1