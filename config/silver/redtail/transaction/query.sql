SELECT
    rec_id,
    glynac_organization_id,
    
    -- Transaction identifiers
    trimBoth(COALESCE(transaction_number, '')) as transaction_number,
    trimBoth(COALESCE(transaction_type, '')) as transaction_type,
    
    -- Foreign keys
    account_id,
    investment_id,
    
    -- Financial amounts (convert string to Decimal with safe handling)
    toDecimal64OrZero(replaceAll(replaceAll(COALESCE(amount, '0'), ',', ''), '$', ''), 2) as amount,
    toDecimal64OrZero(replaceAll(replaceAll(COALESCE(fees, '0'), ',', ''), '$', ''), 2) as fees,
    toDecimal64OrZero(replaceAll(replaceAll(COALESCE(price_per_unit, '0'), ',', ''), '$', ''), 4) as price_per_unit,
    toDecimal64OrZero(replaceAll(replaceAll(COALESCE(quantity, '0'), ',', ''), '$', ''), 4) as quantity,
    
    -- Currency
    trimBoth(COALESCE(currency, 'USD')) as currency,
    
    -- Dates (parse string to Date/DateTime)
    parseDateTime64BestEffortOrNull(toString(transaction_date)) as transaction_date,
    parseDateTime64BestEffortOrNull(toString(settlement_date)) as settlement_date,
    parseDateTime64BestEffortOrNull(toString(entry_date)) as entry_date,
    
    -- Status (map transaction_status to status in model)
    trimBoth(COALESCE(transaction_status, 'Completed')) as status,
    
    -- Status normalization
    multiIf(
        transaction_status ILIKE '%completed%', 'completed',
        transaction_status ILIKE '%pending%', 'pending',
        transaction_status ILIKE '%failed%', 'failed',
        transaction_status ILIKE '%cancelled%', 'cancelled',
        transaction_status != '', Lower(trimBoth(transaction_status)),
        'completed'
    ) as status_normalized,
    
    -- Description and notes
    trimBoth(COALESCE(description, '')) as description,
    trimBoth(COALESCE(notes, '')) as notes,
    trimBoth(COALESCE(reference_number, '')) as reference_number,
    
    -- Flags (convert Boolean to UInt8)
    CAST(is_active AS UInt8) as is_active,
    
    -- Investment data (JSON blob - keep as-is)
    investment_data,
    
    -- Investment field (appears to be duplicate/denormalized field)
    trimBoth(COALESCE(investment, '')) as investment_name,
    
    -- Audit fields
    parseDateTime64BestEffortOrNull(toString(rec_add)) as rec_add,
    rec_add_user,
    parseDateTime64BestEffortOrNull(toString(rec_edit)) as rec_edit,
    rec_edit_user,
    
    -- Batch metadata
    trimBoth(COALESCE(scan_id, '')) as scan_id,
    batch_number,
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'redtail.transaction_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM redtail.transaction_raw
WHERE rec_id IS NOT NULL
  AND rec_id != 0
  AND account_id IS NOT NULL
  AND account_id != 0
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rec_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC, batch_number DESC 
) = 1