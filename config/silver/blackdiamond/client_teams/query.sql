SELECT
    -- User-team relationship identifiers
    user_id,
    trimBoth(COALESCE(team_name, '')) as team_name,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.client_teams_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.client_teams_raw
WHERE user_id IS NOT NULL
  AND team_name IS NOT NULL
  AND team_name != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, user_id, team_name 
    ORDER BY processing_date DESC
) = 1