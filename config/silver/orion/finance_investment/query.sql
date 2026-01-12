SELECT
    -- Primary identifiers
    trimBoth(COALESCE(investment_id, '')) AS investment_id,
    trimBoth(COALESCE(investment_record_id, '')) AS investment_record_id,
    trimBoth(COALESCE(investment_record_type, '')) AS investment_record_type,
    trimBoth(COALESCE(glynac_organization_id, '')) AS glynac_organization_id,

    -- Relationships
    trimBoth(COALESCE(account_id, '')) AS account_id,
    trimBoth(COALESCE(portfolio_id, '')) AS portfolio_id,
    trimBoth(COALESCE(asset_id, '')) AS asset_id,
    trimBoth(COALESCE(asset_name, '')) AS asset_name,

    -- Investment details
    trimBoth(COALESCE(investment_type, '')) AS investment_type,
    trimBoth(COALESCE(investment_status, '')) AS investment_status,
    trimBoth(COALESCE(risk_rating, '')) AS risk_rating,

    -- Financial metrics
    toFloat64OrZero(amount) AS amount,
    trimBoth(COALESCE(currency, '')) AS currency,
    toFloat64OrZero(quantity) AS quantity,
    toFloat64OrZero(purchase_price) AS purchase_price,
    toFloat64OrZero(current_price) AS current_price,
    toFloat64OrZero(current_value) AS current_value,
    toFloat64OrZero(gain_loss) AS gain_loss,
    toFloat64OrZero(gain_loss_percentage) AS gain_loss_percentage,
    toFloat64OrZero(yield_rate) AS yield_rate,
    COALESCE(is_tax_advantaged, false) AS is_tax_advantaged,

    -- Dates (parse string to DateTime)
    parseDateTime64BestEffortOrNull(toString(purchase_date)) AS purchase_date,
    parseDateTime64BestEffortOrNull(toString(maturity_date)) AS maturity_date,
    parseDateTime64BestEffortOrNull(toString(created_date)) AS created_date,
    parseDateTime64BestEffortOrNull(toString(modified_date)) AS modified_date,

    -- Tags
    trimBoth(COALESCE(tags, '')) AS tags,

    -- Processing metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) AS processing_timestamp,

    -- System columns
    now() AS _loaded_at,
    'orion.finance_investment_raw' AS _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) AS _source_timestamp

FROM orion.finance_investment_raw

WHERE investment_id IS NOT NULL
  AND investment_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY investment_id, glynac_organization_id, processing_date
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1;
