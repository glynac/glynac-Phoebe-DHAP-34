SELECT
    -- Entity classification
    trimBoth(COALESCE(entity_type, '')) as entity_type,
    trimBoth(COALESCE(entity_id, '')) as entity_id,
    
    -- Identifier details
    trimBoth(COALESCE(label, '')) as label,
    trimBoth(COALESCE(value, '')) as value,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.contact_outside_identifiers_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.contact_outside_identifiers_raw
WHERE entity_id IS NOT NULL
  AND entity_id != ''
  AND value IS NOT NULL
  AND value != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, entity_id, label, value 
    ORDER BY processing_date DESC
) = 1