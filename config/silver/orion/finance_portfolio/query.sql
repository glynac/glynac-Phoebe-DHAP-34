SELECT
    portfolio_id,
    glynac_organization_id,

    -- Portfolio identification
    trimBoth(COALESCE(name, '')) as name,
    trimBoth(COALESCE(portfolio_type, '')) as portfolio_type,
    trimBoth(COALESCE(portfolio_status, '')) as portfolio_status,

    -- Account and ownership
    trimBoth(COALESCE(account_id, '')) as account_id,
    trimBoth(COALESCE(owner_id, '')) as owner_id,
    trimBoth(COALESCE(owner_name, '')) as owner_name,

    -- Financial details
    toDecimal64OrZero(toString(total_value), 2) as total_value,
    trimBoth(COALESCE(currency, '')) as currency,
    toDecimal64OrZero(toString(cash_balance), 2) as cash_balance,
    toDecimal64OrZero(toString(investment_value), 2) as investment_value,

    -- Investment profile
    trimBoth(COALESCE(risk_profile, '')) as risk_profile,
    trimBoth(COALESCE(investment_objective, '')) as investment_objective,
    trimBoth(COALESCE(benchmark, '')) as benchmark,

    -- Performance
    toDecimal64OrZero(toString(ytd_return), 4) as ytd_return,
    toDecimal64OrZero(toString(one_year_return), 4) as one_year_return,

    -- Additional information
    trimBoth(COALESCE(description, '')) as description,
    trimBoth(COALESCE(tags, '')) as tags,

    -- Raw message metadata
    trimBoth(COALESCE(portfolio_type_field, '')) as portfolio_type_field,
    trimBoth(COALESCE(portfolio_id_field, '')) as portfolio_id_field,
    trimBoth(COALESCE(message_type, '')) as message_type,
    trimBoth(COALESCE(entity_type, '')) as entity_type,
    trimBoth(COALESCE(scan_id, '')) as scan_id,
    toInt32OrZero(toString(batch_number)) as batch_number,
    toInt64OrZero(toString(batch_offset)) as batch_offset,
    parseDateTime64BestEffortOrNull(toString(batch_timestamp)) as batch_timestamp,

    -- Dates
    parseDateTime64BestEffortOrNull(toString(inception_date)) as inception_date,
    parseDateTime64BestEffortOrNull(toString(last_rebalance_date)) as last_rebalance_date,
    parseDateTime64BestEffortOrNull(toString(created_date)) as created_date,
    parseDateTime64BestEffortOrNull(toString(modified_date)) as modified_date,

    -- Batch metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,

    -- System columns
    now() as _loaded_at,
    'orion.finance_portfolio_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM orion.finance_portfolio_raw
WHERE portfolio_id IS NOT NULL
  AND portfolio_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY portfolio_id, glynac_organization_id, processing_date
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1
