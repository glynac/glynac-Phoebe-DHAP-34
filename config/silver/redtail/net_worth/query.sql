SELECT
    rec_id,
    glynac_organization_id,
    
    -- Identifiers
    contact_id,
    
    -- As of date
    parseDateTime64BestEffortOrNull(toString(as_of_date)) as as_of_date,
    
    -- Assets (convert string to decimal, handle currency symbols and commas)
    toDecimal64OrZero(replaceAll(replaceAll(cash_and_equivalents, ',', ''), '$', ''), 2) as cash_and_equivalents,
    toDecimal64OrZero(replaceAll(replaceAll(investments, ',', ''), '$', ''), 2) as investments,
    toDecimal64OrZero(replaceAll(replaceAll(retirement_accounts, ',', ''), '$', ''), 2) as retirement_accounts,
    toDecimal64OrZero(replaceAll(replaceAll(real_estate, ',', ''), '$', ''), 2) as real_estate,
    toDecimal64OrZero(replaceAll(replaceAll(business_interests, ',', ''), '$', ''), 2) as business_interests,
    toDecimal64OrZero(replaceAll(replaceAll(personal_property, ',', ''), '$', ''), 2) as personal_property,
    toDecimal64OrZero(replaceAll(replaceAll(other_assets, ',', ''), '$', ''), 2) as other_assets,
    toDecimal64OrZero(replaceAll(replaceAll(total_assets, ',', ''), '$', ''), 2) as total_assets,
    
    -- Liabilities (convert string to decimal, handle currency symbols and commas)
    toDecimal64OrZero(replaceAll(replaceAll(short_term_liabilities, ',', ''), '$', ''), 2) as short_term_liabilities,
    toDecimal64OrZero(replaceAll(replaceAll(mortgages, ',', ''), '$', ''), 2) as mortgages,
    toDecimal64OrZero(replaceAll(replaceAll(loans, ',', ''), '$', ''), 2) as loans,
    toDecimal64OrZero(replaceAll(replaceAll(other_liabilities, ',', ''), '$', ''), 2) as other_liabilities,
    toDecimal64OrZero(replaceAll(replaceAll(total_liabilities, ',', ''), '$', ''), 2) as total_liabilities,
    
    -- Net worth
    toDecimal64OrZero(replaceAll(replaceAll(net_worth_amount, ',', ''), '$', ''), 2) as net_worth_amount,
    
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
    'redtail.net_worth_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM redtail.net_worth_raw
WHERE rec_id IS NOT NULL
  AND rec_id != 0
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rec_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC, batch_number DESC 
) = 1