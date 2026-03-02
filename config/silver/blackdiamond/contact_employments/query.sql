SELECT
    -- Contact employment identifiers
    trimBoth(COALESCE(contact_id, '')) as contact_id,
    employer_name,
    title,
    trimBoth(COALESCE(occupation, '')) as occupation,
    
    -- Employment dates
    parseDateTime64BestEffortOrNull(toString(start_date)) as start_date,
    end_date,
    
    -- Employment status
    COALESCE(is_current, false) as is_current,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.contact_employments_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.contact_employments_raw
WHERE contact_id IS NOT NULL
  AND contact_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, contact_id, occupation, start_date 
    ORDER BY processing_date DESC
) = 1   