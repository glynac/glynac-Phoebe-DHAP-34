SELECT
    rec_id,
    glynac_organization_id,
    
    -- Portfolio identifiers
    trimBoth(COALESCE(portfolio_name, '')) as portfolio_name,
    trimBoth(COALESCE(portfolio_code, '')) as portfolio_code,
    trimBoth(COALESCE(portfolio_type, '')) as portfolio_type,
    
    -- Strategy and objective
    trimBoth(COALESCE(strategy, '')) as strategy,
    trimBoth(COALESCE(objective, '')) as objective,
    trimBoth(COALESCE(risk_level, '')) as risk_level,
    
    -- Target return (convert string percentage to Decimal)
    CAST(
        replaceAll(replaceAll(COALESCE(target_return, '0'), ',', ''), '%', '') 
        AS Decimal(5, 2)
    ) as target_return,
    
    -- Performance metrics (convert string percentages to Decimal)
    CAST(
        replaceAll(replaceAll(COALESCE(ytd_return, '0'), ',', ''), '%', '') 
        AS Decimal(5, 2)
    ) as ytd_return,
    
    CAST(
        replaceAll(replaceAll(COALESCE(one_year_return, '0'), ',', ''), '%', '') 
        AS Decimal(5, 2)
    ) as one_year_return,
    
    CAST(
        replaceAll(replaceAll(COALESCE(three_year_return, '0'), ',', ''), '%', '') 
        AS Decimal(5, 2)
    ) as three_year_return,
    
    CAST(
        replaceAll(replaceAll(COALESCE(five_year_return, '0'), ',', ''), '%', '') 
        AS Decimal(5, 2)
    ) as five_year_return,
    
    CAST(
        replaceAll(replaceAll(COALESCE(inception_return, '0'), ',', ''), '%', '') 
        AS Decimal(5, 2)
    ) as inception_return,
    
    -- Foreign keys (user references)
    created_by,
    manager_id,
    
    -- Dates (parse string to Date/DateTime)
    parseDateTime64BestEffortOrNull(toString(inception_date)) as inception_date,
    parseDateTime64BestEffortOrNull(toString(rec_add)) as rec_add,
    parseDateTime64BestEffortOrNull(toString(rec_edit)) as rec_edit,
    
    -- Description and notes
    trimBoth(COALESCE(description, '')) as description,
    trimBoth(COALESCE(notes, '')) as notes,
    
    -- Flags
    is_model,
    is_active,
    
    -- Audit fields
    rec_add_user,
    rec_edit_user,
    
    -- JSON data blobs (keep as-is)
    accounts_data,
    portfolio_allocations_data,
    
    -- Batch metadata
    trimBoth(COALESCE(scan_id, '')) as scan_id,
    batch_number,
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as processing_timestamp,
    
    -- System columns
    now() as _loaded_at,
    'redtail.portfolio_raw' as _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) as _source_timestamp

FROM redtail.portfolio_raw
WHERE rec_id IS NOT NULL
  AND rec_id != 0
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rec_id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC, batch_number DESC 
) = 1