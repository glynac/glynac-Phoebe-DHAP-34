SELECT
    -- Relationship identifier
    trimBoth(COALESCE(id, '')) as id,
    
    -- Relationship names
    trimBoth(COALESCE(name, '')) as name,
    trimBoth(COALESCE(display_name, '')) as display_name,
    
    -- Dates
    parseDateTime64BestEffortOrNull(toString(inception_date)) as inception_date,
    termination_date,
    
    -- CRM and classification
    trimBoth(COALESCE(crm_source, '')) as crm_source,
    client_type_id,
    
    -- Financial team template
    financial_team_template_id,
    financial_team_template_name,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.relationships_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.relationships_raw
WHERE id IS NOT NULL
  AND id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, id, processing_date 
    ORDER BY parseDateTime64BestEffortOrNull(toString(inception_date)) DESC
) = 1