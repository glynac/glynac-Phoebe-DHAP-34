SELECT
    -- Account identifiers
    trimBoth(COALESCE(id, '')) as id,
    trimBoth(COALESCE(name, '')) as name,
    trimBoth(COALESCE(custodial_account_name, '')) as custodial_account_name,
    trimBoth(COALESCE(account_number, '')) as account_number,
    trimBoth(COALESCE(billing_number, '')) as billing_number,
    trimBoth(COALESCE(long_number, '')) as long_number,
    
    -- Account classification
    trimBoth(COALESCE(account_type, '')) as account_type,
    trimBoth(COALESCE(account_category, '')) as account_category,
    trimBoth(COALESCE(account_sub_category, '')) as account_sub_category,
    
    -- Tax information
    tax_methodology,
    tax_status,
    trimBoth(COALESCE(account_registration_type, '')) as account_registration_type,
    
    -- Dates (parse string to DateTime)
    parseDateTime64BestEffortOrNull(toString(start_date)) as start_date,
    closed_date,
    parseDateTime64BestEffortOrNull(toString(as_of_date)) as as_of_date,
    parseDateTime64BestEffortOrNull(toString(billing_start_date)) as billing_start_date,
    parseDateTime64BestEffortOrNull(toString(billing_end_date)) as billing_end_date,
    history_start_date,
    parseDateTime64BestEffortOrNull(toString(last_reconciled_date)) as last_reconciled_date,
    parseDateTime64BestEffortOrNull(toString(performance_start_date)) as performance_start_date,
    parseDateTime64BestEffortOrNull(toString(create_date)) as create_date,
    
    -- Custodian and provider information
    trimBoth(COALESCE(custodian, '')) as custodian,
    trimBoth(COALESCE(data_provider, '')) as data_provider,
    data_provider_id,
    manager_strategy_name,
    
    -- Account flags
    COALESCE(discretionary, false) as discretionary,
    COALESCE(billable, false) as billable,
    COALESCE(supervised, false) as supervised,
    COALESCE(position_only, false) as position_only,
    COALESCE(is_sleeve_account, false) as is_sleeve_account,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.accounts_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.accounts_raw
WHERE id IS NOT NULL
  AND id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(as_of_date)) DESC
) = 1