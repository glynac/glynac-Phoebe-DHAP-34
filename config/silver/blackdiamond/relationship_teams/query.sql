SELECT
    -- Mapping keys
    trimBoth(COALESCE(relationship_id, '')) as relationship_id,
    trimBoth(COALESCE(team_id, '')) as team_id,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.relationship_teams_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.relationship_teams_raw
WHERE relationship_id IS NOT NULL
  AND relationship_id != ''
  AND team_id IS NOT NULL
  AND team_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, relationship_id, team_id, processing_date 
    ORDER BY 1
) = 1