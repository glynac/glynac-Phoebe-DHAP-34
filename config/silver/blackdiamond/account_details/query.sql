SELECT
    -- Account identifier
    trimBoth(COALESCE(account_id, '')) as account_id,
    
    -- Dates (parse string to DateTime)
    parseDateTime64BestEffortOrNull(toString(as_of_date)) as as_of_date,
    parseDateTime64BestEffortOrNull(toString(max_account_as_of_date)) as max_account_as_of_date,
    
    -- Market values (already Float64, keep as-is)
    COALESCE(total_emv, 0.0) as total_emv,
    COALESCE(supervised_emv, 0.0) as supervised_emv,
    COALESCE(unsupervised_emv, 0.0) as unsupervised_emv,
    
    -- Realized gains/losses (nullable Int32)
    realized_short_term_gain_loss,
    realized_long_term_gain_loss,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.account_details_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.account_details_raw
WHERE account_id IS NOT NULL
  AND account_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, account_id, processing_date 
    ORDER BY as_of_date DESC
) = 1