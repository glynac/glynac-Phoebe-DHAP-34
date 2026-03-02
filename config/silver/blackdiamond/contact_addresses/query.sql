SELECT
    -- Contact address identifiers
    trimBoth(COALESCE(contact_id, '')) as contact_id,
    trimBoth(COALESCE(label, '')) as label,
    toFloat64OrZero(ordinal) as ordinal,
    
    -- Address classification
    COALESCE(mailing_address, false) as mailing_address,
    
    -- Street address components
    trimBoth(COALESCE(street_line1, '')) as street_line1,
    trimBoth(COALESCE(street_line2, '')) as street_line2,
    trimBoth(COALESCE(street_line3, '')) as street_line3,
    street_line4,
    street_line5,
    
    -- Geographic components
    trimBoth(COALESCE(municipality, '')) as municipality,
    trimBoth(COALESCE(administrative_area, '')) as administrative_area,
    trimBoth(COALESCE(sub_administrative_area, '')) as sub_administrative_area,
    trimBoth(COALESCE(postal_code, '')) as postal_code,
    trimBoth(COALESCE(country_code, '')) as country_code,
    
    -- Organization and partition keys
    glynac_organization_id,
    processing_date,
    
    -- System columns
    now() as _loaded_at,
    'blackdiamond.contact_addresses_raw' as _source_table,
    processing_date as _source_timestamp

FROM blackdiamond.contact_addresses_raw
WHERE contact_id IS NOT NULL
  AND contact_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY glynac_organization_id, contact_id, label, ordinal 
    ORDER BY processing_date DESC
) = 1