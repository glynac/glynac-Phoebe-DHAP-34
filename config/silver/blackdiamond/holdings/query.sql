SELECT
    -- Holding identifier
    trimBoth(COALESCE(id, '')) as id,

    -- Account and asset references
    trimBoth(COALESCE(account_id, '')) as account_id,
    toInt64OrZero(toString(asset_id)) as asset_id,

    -- As-of date (Nullable(String) in raw -> Nullable(DateTime64))
    parseDateTime64BestEffortOrNull(as_of_date) as as_of_date,

    -- Position details (Nullable(Int64/Float64) in raw)
    toInt64OrZero(toString(units)) as units,
    toDecimal64OrZero(toString(market_value), 4) as market_value,

    -- Holding flags
    COALESCE(discretionary, false) as discretionary,
    COALESCE(supervised, false) as supervised,
    COALESCE(billable, false) as billable,
    COALESCE(money_market, false) as money_market,
    COALESCE(cash, false) as cash,
    COALESCE(long_holding, false) as long_holding,
    COALESCE(is_manual, false) as is_manual,

    -- Discretion and authority
    trimBoth(COALESCE(investment_discretion, '')) as investment_discretion,
    trimBoth(COALESCE(voting_authority, '')) as voting_authority,

    -- Pricing factors (default 1.0 when null)
    if(price_factor IS NULL, toDecimal64('1.000000', 6), toDecimal64OrZero(toString(price_factor), 6)) as price_factor,
    if(paydown_factor IS NULL, toDecimal64('1.000000', 6), toDecimal64OrZero(toString(paydown_factor), 6)) as paydown_factor,

    -- Fixed income attributes (Int32 in raw)
    toInt32OrZero(toString(coupon_rate)) as coupon_rate,
    toInt32OrZero(toString(muni_state)) as muni_state,
    toInt32OrZero(toString(call_date)) as call_date,
    toDecimal64OrZero(toString(yield), 6) as yield,
    trimBoth(COALESCE(maturity_date, '')) as maturity_date,
    toDecimal64OrZero(toString(accrual), 6) as accrual,

    -- Organization and partition keys
    glynac_organization_id,
    processing_date,

    -- System columns
    now() as _loaded_at,
    'blackdiamond.holdings_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.holdings_raw
WHERE id IS NOT NULL AND id != ''
  AND account_id IS NOT NULL AND account_id != ''
  AND glynac_organization_id IS NOT NULL AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, id, processing_date
    ORDER BY as_of_date DESC
) = 1
