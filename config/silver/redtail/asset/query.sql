SELECT
    rec_id,
    glynac_organization_id,
    
    -- Foreign keys to account and investment
    account_id,
    investment_id,
    
    -- Asset ID (might be same as rec_id, but keeping for reference)
    asset_id,
    
    -- Holding quantity (convert string to Decimal)
    CAST(
        replaceAll(replaceAll(COALESCE(quantity, '0'), ',', ''), '$', '') 
        AS Decimal(15, 4)
    ) as quantity,
    
    -- Cost basis information
    CAST(
        replaceAll(replaceAll(COALESCE(cost_basis, '0'), ',', ''), '$', '') 
        AS Decimal(15, 2)
    ) as cost_basis,
    
    CAST(
        replaceAll(replaceAll(COALESCE(cost_basis_per_unit, '0'), ',', ''), '$', '') 
        AS Decimal(15, 4)
    ) as cost_basis_per_unit,
    
    -- Current pricing
    CAST(
        replaceAll(replaceAll(COALESCE(current_price, '0'), ',', ''), '$', '') 
        AS Decimal(15, 4)
    ) as current_price,
    
    CAST(
        replaceAll(replaceAll(COALESCE(current_value, '0'), ',', ''), '$', '') 
        AS Decimal(15, 2)
    ) as current_value,
    
    -- Gain/loss calculations
    CAST(
        replaceAll(replaceAll(COALESCE(unrealized_gain_loss, '0'), ',', ''), '$', '') 
        AS Decimal(15, 2)
    ) as unrealized_gain_loss,
    
    CAST(
        replaceAll(replaceAll(COALESCE(unrealized_gain_loss_percent, '0'), ',', ''), '%', '') 
        AS Decimal(8, 2)
    ) as unrealized_gain_loss_percent,
    
    -- Allocation percentages
    CAST(
        replaceAll(replaceAll(COALESCE(allocation_percent, '0'), ',', ''), '%', '') 
        AS Decimal(5, 2)
    ) as allocation_percent,
    
    CAST(
        replaceAll(replaceAll(COALESCE(target_allocation, '0'), ',', ''), '%', '') 
        AS Decimal(5, 2)
    ) as target_allocation,
    
    -- Dates (parse string to Date/DateTime)
    parseDateTime64BestEffortOrNull(toString(purchase_date)) as purchase_date,
    parseDateTime64BestEffortOrNull(toString(last_update_date)) as last_update_date,
    
    -- Flags
    is_core_position,
    is_active,
    
    -- Audit fields
    parseDateTime64BestEffortOrNull(toString(rec_add)) as rec_add,
    rec_add_user,
    parseDateTime64BestEffortOrNull(toString(rec_edit)) as rec_edit,
    rec_edit_user,
    
    -- Investment data (JSON blob - keep as-is)
    investment_data,
    
    -- Notes
    trimBoth(COALESCE(notes, '')) as notes,
    
    -- Batch metadata
    trimBoth(COALESCE(scan_id, '')) as scan_id,
    batch_number,
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'redtail.asset_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM redtail.asset_raw
WHERE rec_id IS NOT NULL
  AND rec_id != 0
  AND account_id IS NOT NULL
  AND account_id != 0
  AND investment_id IS NOT NULL
  AND investment_id != 0
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rec_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC, batch_number DESC 
) = 1