SELECT
    -- Primary identifier (String in raw)
    trimBoth(COALESCE(transaction_id, '')) as transaction_id,

    -- Account and holding references (Int32 in raw)
    trimBoth(COALESCE(holding_id, '')) as holding_id,
    toInt32OrZero(toString(account_id)) as account_id,
    toInt32OrZero(toString(account_number)) as account_number,

    -- Asset identification
    trimBoth(COALESCE(asset_id, '')) as asset_id,
    trimBoth(COALESCE(ticker, '')) as ticker,
    trimBoth(COALESCE(cusip, '')) as cusip,
    toInt32OrZero(toString(display_cusip)) as display_cusip,
    toInt32OrZero(toString(alternate_id)) as alternate_id,

    -- Financial values (Float64 in raw -> Decimal for precision)
    toDecimal64OrZero(toString(market_value), 4) as market_value,
    toDecimal64OrZero(toString(units), 8) as units,
    toDecimal64OrZero(toString(price), 6) as price,
    toInt64OrZero(toString(transaction_fee)) as transaction_fee,
    toInt64OrZero(toString(account_fee)) as account_fee,

    -- Transaction codes (Int32 in raw)
    toInt32OrZero(toString(sub_code)) as sub_code,
    trimBoth(COALESCE(trans_code, '')) as trans_code,
    trimBoth(COALESCE(transaction_type, '')) as transaction_type,
    trimBoth(COALESCE(transaction_sub_type, '')) as transaction_sub_type,

    -- Descriptive fields (Int32 in raw, semantically text)
    if(description IS NULL OR description = 0, '', toString(description)) as description,
    if(file_code_description IS NULL OR file_code_description = 0, '', toString(file_code_description)) as file_code_description,
    if(notes IS NULL OR notes = 0, '', toString(notes)) as notes,

    -- Dates (Int32 YYYYMMDD format in raw -> Nullable(Date))
    toDateOrNull(toString(return_date)) as return_date,
    toDateOrNull(toString(settle_date)) as settle_date,
    toDateOrNull(toString(trade_date)) as trade_date,

    -- External references
    trimBoth(COALESCE(external_contra_account_id, '')) as external_contra_account_id,
    toInt32OrZero(toString(action)) as action,
    toInt32OrZero(toString(external_flow_affect)) as external_flow_affect,
    toInt32OrZero(toString(issue_type)) as issue_type,

    -- Organization and partition keys
    glynac_organization_id,
    processing_date,

    -- System columns
    now() as _loaded_at,
    'blackdiamond.transactions_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.transactions_raw
WHERE transaction_id IS NOT NULL AND transaction_id != ''
  AND glynac_organization_id IS NOT NULL AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY transaction_id, glynac_organization_id, processing_date
    ORDER BY toDateOrNull(toString(trade_date)) DESC NULLS LAST,
             toDateOrNull(toString(settle_date)) DESC NULLS LAST
) = 1
