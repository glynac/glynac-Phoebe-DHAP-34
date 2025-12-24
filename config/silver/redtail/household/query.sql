SELECT
    rec_id,
    glynac_organization_id,
    
    -- Household identifiers
    household_id,
    trimBoth(COALESCE(household_name, '')) as household_name,
    trimBoth(COALESCE(household_number, '')) as household_number,
    primary_contact_id,
    
    -- Household classification
    trimBoth(COALESCE(household_type, '')) as household_type,
    trimBoth(COALESCE(household_status, '')) as household_status,
    
    -- Status normalization
    multiIf(
        household_status ILIKE '%active%', 'active',
        household_status ILIKE '%inactive%', 'inactive',
        household_status ILIKE '%archived%', 'archived',
        household_status != '', toLower(trimBoth(household_status)),
        'active'
    ) as status_normalized,
    
    -- Financial data
    toDecimal64OrZero(replaceAll(replaceAll(total_net_worth, ',', ''), '$', ''), 2) as total_net_worth,
    toDecimal64OrZero(replaceAll(replaceAll(total_income, ',', ''), '$', ''), 2) as total_income,
    
    -- Address information
    trimBoth(COALESCE(address1, '')) as address_line1,
    trimBoth(COALESCE(address2, '')) as address_line2,
    trimBoth(COALESCE(city, '')) as city,
    trimBoth(COALESCE(state, '')) as state,
    trimBoth(COALESCE(zip_code, '')) as zip_code,
    trimBoth(COALESCE(country, 'USA')) as country,
    
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
    'redtail.household_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM redtail.household_raw
WHERE rec_id IS NOT NULL
  AND rec_id != 0
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rec_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC, batch_number DESC 
) = 1