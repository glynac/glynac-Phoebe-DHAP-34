-- Silver transformation for microsoft.calendar_event_raw
-- Normalizes the entity_data JSON blob into typed columns.
--
-- NOTE: entity_data contains non-standard JSON (uppercase TRUE/FALSE booleans,
-- unescaped newlines in string values), which breaks JSONExtractString/Bool/Int.
-- We use simpleJSONExtractString for strings (works on non-strict JSON),
-- simpleJSONExtractRaw for booleans (compare raw value to 'TRUE'),
-- and toInt32OrZero(simpleJSONExtractRaw) for integers.
--
-- NOTE: event_id inside entity_data is always empty "". We use web_link
-- as the unique event identifier for deduplication (populated on all records).
SELECT
    -- Record metadata from Bronze
    trimBoth(COALESCE(record_id, ''))              AS record_id,
    trimBoth(COALESCE(glynac_organization_id, '')) AS glynac_organization_id,
    trimBoth(COALESCE(tenant_id, ''))              AS tenant_id,
    is_active,

    -- ── Unique event identifier (event_id is empty in source; web_link is the real PK) ──
    trimBoth(simpleJSONExtractString(entity_data, 'web_link'))         AS event_web_link,

    -- ── Core event identity ───────────────────────────────────────────────────
    trimBoth(simpleJSONExtractString(entity_data, 'event_id'))         AS event_id,
    trimBoth(simpleJSONExtractString(entity_data, 'icalendar_uid'))    AS icalendar_uid,

    -- Calendar reference
    trimBoth(simpleJSONExtractString(entity_data, 'calendar_id'))           AS calendar_id,
    trimBoth(simpleJSONExtractString(entity_data, 'calendar_name'))         AS calendar_name,
    trimBoth(simpleJSONExtractString(entity_data, 'calendar_owner_email'))  AS calendar_owner_email,
    trimBoth(simpleJSONExtractRaw(entity_data, 'calendar_owner_id'))        AS calendar_owner_id,

    -- Event content
    trimBoth(simpleJSONExtractString(entity_data, 'subject'))               AS subject,
    trimBoth(simpleJSONExtractString(entity_data, 'body_preview'))          AS body_preview,
    trimBoth(simpleJSONExtractString(entity_data, 'body_content_type'))     AS body_content_type,

    -- Timing
    parseDateTime64BestEffortOrNull(simpleJSONExtractString(entity_data, 'start_datetime'))  AS start_datetime,
    parseDateTime64BestEffortOrNull(simpleJSONExtractString(entity_data, 'end_datetime'))    AS end_datetime,
    trimBoth(simpleJSONExtractString(entity_data, 'start_timezone'))         AS start_timezone,
    trimBoth(simpleJSONExtractString(entity_data, 'end_timezone'))           AS end_timezone,
    simpleJSONExtractRaw(entity_data, 'is_all_day') = 'TRUE'                AS is_all_day,
    toInt32OrZero(simpleJSONExtractRaw(entity_data, 'duration_minutes'))    AS duration_minutes,

    -- Location
    trimBoth(simpleJSONExtractString(entity_data, 'location'))               AS location,
    trimBoth(simpleJSONExtractString(entity_data, 'location_display_name'))  AS location_display_name,
    trimBoth(simpleJSONExtractString(entity_data, 'location_type'))          AS location_type,

    -- Organizer
    trimBoth(simpleJSONExtractString(entity_data, 'organizer_email'))  AS organizer_email,
    trimBoth(simpleJSONExtractString(entity_data, 'organizer_name'))   AS organizer_name,
    trimBoth(simpleJSONExtractRaw(entity_data, 'organizer_id'))        AS organizer_id,

    -- Attendee counts
    toInt32OrZero(simpleJSONExtractRaw(entity_data, 'attendee_count'))           AS attendee_count,
    toInt32OrZero(simpleJSONExtractRaw(entity_data, 'required_attendee_count'))  AS required_attendee_count,
    toInt32OrZero(simpleJSONExtractRaw(entity_data, 'optional_attendee_count'))  AS optional_attendee_count,
    toInt32OrZero(simpleJSONExtractRaw(entity_data, 'resource_count'))           AS resource_count,

    -- Online meeting
    simpleJSONExtractRaw(entity_data, 'is_online_meeting') = 'TRUE'             AS is_online_meeting,
    trimBoth(simpleJSONExtractString(entity_data, 'online_meeting_provider'))   AS online_meeting_provider,
    trimBoth(simpleJSONExtractString(entity_data, 'online_meeting_url'))        AS online_meeting_url,

    -- Status / flags
    trimBoth(simpleJSONExtractString(entity_data, 'show_as'))       AS show_as,
    trimBoth(simpleJSONExtractString(entity_data, 'sensitivity'))   AS sensitivity,
    trimBoth(simpleJSONExtractString(entity_data, 'importance'))    AS importance,
    simpleJSONExtractRaw(entity_data, 'is_cancelled')  = 'TRUE'    AS is_cancelled,
    simpleJSONExtractRaw(entity_data, 'is_draft')      = 'TRUE'    AS is_draft,
    simpleJSONExtractRaw(entity_data, 'is_organizer')  = 'TRUE'    AS is_organizer,
    simpleJSONExtractRaw(entity_data, 'is_recurring')  = 'TRUE'    AS is_recurring,
    trimBoth(simpleJSONExtractString(entity_data, 'series_master_id'))  AS series_master_id,
    simpleJSONExtractRaw(entity_data, 'is_exception')  = 'TRUE'    AS is_exception,

    -- Attachments / categories
    simpleJSONExtractRaw(entity_data, 'has_attachments') = 'TRUE'      AS has_attachments,
    toInt32OrZero(simpleJSONExtractRaw(entity_data, 'attachment_count')) AS attachment_count,
    toInt32OrZero(simpleJSONExtractRaw(entity_data, 'category_count'))   AS category_count,

    -- Response / links
    trimBoth(simpleJSONExtractString(entity_data, 'response_status'))  AS response_status,

    -- Audit timestamps from entity_data
    parseDateTime64BestEffortOrNull(simpleJSONExtractString(entity_data, 'created_datetime'))       AS created_datetime,
    parseDateTime64BestEffortOrNull(simpleJSONExtractString(entity_data, 'last_modified_datetime')) AS last_modified_datetime,

    -- Processing metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) AS processing_timestamp,

    -- System columns
    now()                             AS _loaded_at,
    'microsoft.calendar_event_raw'    AS _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) AS _source_timestamp

FROM microsoft.calendar_event_raw
WHERE entity_data IS NOT NULL
  AND entity_data != ''
  AND simpleJSONExtractString(entity_data, 'web_link') != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY simpleJSONExtractString(entity_data, 'web_link'), glynac_organization_id, processing_date
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1
