-- query.sql
SELECT
    -- Contact identifiers
    trimBoth(COALESCE(id, '')) as id,
    internal_id,
    
    -- Name components
    trimBoth(COALESCE(salutation, '')) as salutation,
    trimBoth(COALESCE(first_name, '')) as first_name,
    trimBoth(COALESCE(middle_name, '')) as middle_name,
    trimBoth(COALESCE(last_name, '')) as last_name,
    trimBoth(COALESCE(suffix, '')) as suffix,
    trimBoth(COALESCE(nickname, '')) as nickname,
    trimBoth(COALESCE(display_name, '')) as display_name,
    trimBoth(COALESCE(full_name, '')) as full_name,
    
    -- Demographic information
    parseDateTime64BestEffortOrNull(toString(date_of_birth)) as date_of_birth,
    parseDateTime64BestEffortOrNull(toString(date_of_death)) as date_of_death,
    trimBoth(COALESCE(gender, '')) as gender,
    trimBoth(COALESCE(marital_status, '')) as marital_status,
    
    -- Financial and retirement information
    toFloat64OrZero(income) as income,
    toFloat64OrZero(retirement_age) as retirement_age,
    parseDateTime64BestEffortOrNull(toString(actual_retirement_date)) as actual_retirement_date,
    COALESCE(is_disabled, false) as is_disabled,
    
    -- Team assignment
    trimBoth(COALESCE(team_id, '')) as team_id,
    toFloat64OrZero(team_internal_id) as team_internal_id,
    trimBoth(COALESCE(team_name, '')) as team_name,
    
    -- User association
    toFloat64OrZero(user_id) as user_id,
    user_login,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.contact_profiles_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.contact_profiles_raw
WHERE id IS NOT NULL
  AND id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, id 
    ORDER BY processing_date DESC
) = 1