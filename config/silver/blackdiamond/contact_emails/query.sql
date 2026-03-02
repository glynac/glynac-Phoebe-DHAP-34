SELECT
    -- Contact email identifiers
    trimBoth(COALESCE(contact_id, '')) as contact_id,
    trimBoth(COALESCE(label, '')) as label,
    trimBoth(COALESCE(email, '')) as email,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.contact_emails_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.contact_emails_raw
WHERE contact_id IS NOT NULL
  AND contact_id != ''
  AND email IS NOT NULL
  AND email != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, contact_id, email 
    ORDER BY processing_date DESC
) = 1