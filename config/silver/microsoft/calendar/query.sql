-- Silver transformation for microsoft.calendar_raw
-- NOTE: event_id, user_upn, title and all event-content fields are NULL in the
-- current Bronze data (upstream Flink pipeline has not populated them yet).
-- We use scan_id as the unique identifier instead of event_id.
-- is_virtual and allow_new_time_proposals are Nullable(Bool) — coalesced to false.
SELECT
    -- Scan identifier (primary key — event_id is NULL in current source data)
    trimBoth(COALESCE(scan_id, ''))                AS scan_id,
    trimBoth(COALESCE(glynac_organization_id, '')) AS glynac_organization_id,

    -- Tenant / user
    trimBoth(COALESCE(tenant_id, ''))              AS tenant_id,
    trimBoth(COALESCE(user_upn, ''))               AS user_upn,

    -- Core event identifier (NULL in current source; populated when upstream is fixed)
    trimBoth(COALESCE(event_id, ''))               AS event_id,

    -- Organizer
    trimBoth(COALESCE(organizer_name, ''))         AS organizer_name,
    trimBoth(COALESCE(organizer_email, ''))        AS organizer_email,
    trimBoth(COALESCE(organizer_response, ''))     AS organizer_response,

    -- Event details
    trimBoth(COALESCE(title, ''))                  AS title,
    trimBoth(COALESCE(description, ''))            AS description,
    trimBoth(COALESCE(location, ''))               AS location,
    COALESCE(is_virtual, false)                    AS is_virtual,
    COALESCE(allow_new_time_proposals, false)      AS allow_new_time_proposals,

    -- Date/time fields
    parseDateTime64BestEffortOrNull(toString(start_time))           AS start_time,
    parseDateTime64BestEffortOrNull(toString(end_time))             AS end_time,
    parseDateTime64BestEffortOrNull(toString(created_at))           AS created_at,
    parseDateTime64BestEffortOrNull(toString(updated_at))           AS updated_at,

    -- Processing metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) AS processing_timestamp,

    -- System columns
    now()                                                           AS _loaded_at,
    'microsoft.calendar_raw'                                        AS _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) AS _source_timestamp

FROM microsoft.calendar_raw
WHERE scan_id IS NOT NULL
  AND scan_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY scan_id, tenant_id, glynac_organization_id, processing_date
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1
