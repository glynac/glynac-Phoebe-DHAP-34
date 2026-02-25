-- Silver transformation for microsoft.calendar_event_raw
-- Normalizes the entity_data JSON blob into typed columns
SELECT
    -- Record metadata from Bronze
    trimBoth(COALESCE(record_id, ''))              AS record_id,
    trimBoth(COALESCE(glynac_organization_id, '')) AS glynac_organization_id,
    trimBoth(COALESCE(tenant_id, ''))              AS tenant_id,
    is_active,

    -- ── Core event identity (from entity_data JSON) ──────────────────────────
    trimBoth(JSONExtractString(entity_data, 'event_id'))         AS event_id,
    trimBoth(JSONExtractString(entity_data, 'icalendar_uid'))    AS icalendar_uid,

    -- Calendar reference
    trimBoth(JSONExtractString(entity_data, 'calendar_id'))           AS calendar_id,
    trimBoth(JSONExtractString(entity_data, 'calendar_name'))         AS calendar_name,
    trimBoth(JSONExtractString(entity_data, 'calendar_owner_email'))  AS calendar_owner_email,
    trimBoth(JSONExtractString(entity_data, 'calendar_owner_id'))     AS calendar_owner_id,

    -- Event content
    trimBoth(JSONExtractString(entity_data, 'subject'))          AS subject,
    trimBoth(JSONExtractString(entity_data, 'body_preview'))     AS body_preview,
    trimBoth(JSONExtractString(entity_data, 'body_content_type')) AS body_content_type,

    -- Timing
    parseDateTime64BestEffortOrNull(JSONExtractString(entity_data, 'start_datetime'))  AS start_datetime,
    parseDateTime64BestEffortOrNull(JSONExtractString(entity_data, 'end_datetime'))    AS end_datetime,
    trimBoth(JSONExtractString(entity_data, 'start_timezone'))   AS start_timezone,
    trimBoth(JSONExtractString(entity_data, 'end_timezone'))     AS end_timezone,
    JSONExtractBool(entity_data, 'is_all_day')                   AS is_all_day,
    JSONExtractInt(entity_data, 'duration_minutes')              AS duration_minutes,

    -- Location
    trimBoth(JSONExtractString(entity_data, 'location'))              AS location,
    trimBoth(JSONExtractString(entity_data, 'location_display_name')) AS location_display_name,
    trimBoth(JSONExtractString(entity_data, 'location_type'))         AS location_type,

    -- Organizer
    trimBoth(JSONExtractString(entity_data, 'organizer_email')) AS organizer_email,
    trimBoth(JSONExtractString(entity_data, 'organizer_name'))  AS organizer_name,
    trimBoth(JSONExtractString(entity_data, 'organizer_id'))    AS organizer_id,

    -- Attendee counts
    JSONExtractInt(entity_data, 'attendee_count')           AS attendee_count,
    JSONExtractInt(entity_data, 'required_attendee_count')  AS required_attendee_count,
    JSONExtractInt(entity_data, 'optional_attendee_count')  AS optional_attendee_count,
    JSONExtractInt(entity_data, 'resource_count')           AS resource_count,

    -- Online meeting
    JSONExtractBool(entity_data, 'is_online_meeting')                  AS is_online_meeting,
    trimBoth(JSONExtractString(entity_data, 'online_meeting_provider')) AS online_meeting_provider,
    trimBoth(JSONExtractString(entity_data, 'online_meeting_url'))      AS online_meeting_url,

    -- Status / flags
    trimBoth(JSONExtractString(entity_data, 'show_as'))      AS show_as,
    trimBoth(JSONExtractString(entity_data, 'sensitivity'))  AS sensitivity,
    trimBoth(JSONExtractString(entity_data, 'importance'))   AS importance,
    JSONExtractBool(entity_data, 'is_cancelled')             AS is_cancelled,
    JSONExtractBool(entity_data, 'is_draft')                 AS is_draft,
    JSONExtractBool(entity_data, 'is_organizer')             AS is_organizer,
    JSONExtractBool(entity_data, 'is_recurring')             AS is_recurring,
    trimBoth(JSONExtractString(entity_data, 'series_master_id')) AS series_master_id,
    JSONExtractBool(entity_data, 'is_exception')             AS is_exception,

    -- Attachments / categories
    JSONExtractBool(entity_data, 'has_attachments')         AS has_attachments,
    JSONExtractInt(entity_data, 'attachment_count')         AS attachment_count,
    JSONExtractInt(entity_data, 'category_count')           AS category_count,

    -- Response / links
    trimBoth(JSONExtractString(entity_data, 'response_status')) AS response_status,
    trimBoth(JSONExtractString(entity_data, 'web_link'))         AS web_link,

    -- Audit timestamps from entity_data
    parseDateTime64BestEffortOrNull(JSONExtractString(entity_data, 'created_datetime'))       AS created_datetime,
    parseDateTime64BestEffortOrNull(JSONExtractString(entity_data, 'last_modified_datetime')) AS last_modified_datetime,

    -- Processing metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) AS processing_timestamp,

    -- System columns
    now()                             AS _loaded_at,
    'microsoft.calendar_event_raw'    AS _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) AS _source_timestamp

FROM microsoft.calendar_event_raw
WHERE entity_data IS NOT NULL
  AND isValidJSON(entity_data)
  AND JSONExtractString(entity_data, 'event_id') != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY JSONExtractString(entity_data, 'event_id'), glynac_organization_id, processing_date
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1
