SELECT
    -- Asset identifiers
    asset_id,
    trimBoth(COALESCE(asset_name, '')) as asset_name,
    trimBoth(COALESCE(asset_name_short, '')) as asset_name_short,
    trimBoth(COALESCE(ticker, '')) as ticker,
    trimBoth(COALESCE(cusip, '')) as cusip,
    trimBoth(COALESCE(display_cusip, '')) as display_cusip,
    trimBoth(COALESCE(alternate_id, '')) as alternate_id,
    
    -- Asset classification hierarchy
    super_class_id,
    trimBoth(COALESCE(superclass_name, '')) as superclass_name,
    super_class_ordinal,
    class_id,
    trimBoth(COALESCE(class_name, '')) as class_name,
    class_ordinal,
    sector_segment_id,
    trimBoth(COALESCE(sector_segment, '')) as sector_segment,
    sector_segment_ordinal,
    segment_id,
    trimBoth(COALESCE(segment_name, '')) as segment_name,
    cap_segment_id,
    cap_segment_ordinal,
    
    -- Asset type information
    trimBoth(COALESCE(provider_issue_type, '')) as provider_issue_type,
    trimBoth(COALESCE(sec_asset_type, '')) as sec_asset_type,
    
    -- Asset flags
    COALESCE(cash, false) as cash,
    COALESCE(is_cash_equivalent, false) as is_cash_equivalent,
    COALESCE(money_market, false) as money_market,
    COALESCE(archived, false) as archived,
    COALESCE(billable, false) as billable,
    COALESCE(supervised, false) as supervised,
    COALESCE(discretionary, false) as discretionary,
    
    -- Financial attributes
    coupon_rate,
    last_dividend,
    maturity_date,
    first_payment_date,
    issue_date,
    call_date,
    trimBoth(COALESCE(tax_status, '')) as tax_status,
    trimBoth(COALESCE(paydown_factor, '')) as paydown_factor,
    COALESCE(price_factor, 1.0) as price_factor,
    muni_state,
    COALESCE(yield, 0.0) as yield,
    
    -- Exchange information
    exchange,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.assets_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.assets_raw
WHERE asset_id IS NOT NULL
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, asset_id, processing_date 
    ORDER BY asset_id DESC
) = 1