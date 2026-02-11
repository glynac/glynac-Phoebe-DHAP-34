SELECT
    -- Relationship and account references
    trimBoth(COALESCE(relationship_id, '')) as relationship_id,
    trimBoth(COALESCE(account_id, '')) as account_id,
    
    -- Configuration flags
    COALESCE(include_in_net_worth, false) as include_in_net_worth,
    COALESCE(is_direct_assigned, false) as is_direct_assigned,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.relationship_accounts_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.relationship_accounts_raw
WHERE relationship_id IS NOT NULL
  AND relationship_id != ''
  AND account_id IS NOT NULL
  AND account_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, relationship_id, account_id, processing_date 
    ORDER BY COALESCE(include_in_net_worth, false) DESC
) = 1