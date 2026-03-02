SELECT
    -- Contact phone identifiers
    trimBoth(COALESCE(contact_id, '')) as contact_id,
    trimBoth(COALESCE(label, '')) as label,
    trimBoth(COALESCE(number, '')) as number,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.contact_phones_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.contact_phones_raw
WHERE contact_id IS NOT NULL
  AND contact_id != ''
  AND number IS NOT NULL
  AND number != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, contact_id, number 
    ORDER BY processing_date DESC
) = 1