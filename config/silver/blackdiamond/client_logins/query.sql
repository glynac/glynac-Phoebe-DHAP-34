SELECT
    -- User identifier
    user_id,
    
    -- Login timestamp (parse string to DateTime)
    parseDateTime64BestEffortOrNull(toString(login_date)) as login_date,
    
    -- Session information
    ip_address,
    device,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.client_logins_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.client_logins_raw
WHERE user_id IS NOT NULL
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, user_id, login_date, processing_date 
    ORDER BY login_date DESC
) = 1