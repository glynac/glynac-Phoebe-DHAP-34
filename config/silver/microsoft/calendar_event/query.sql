-- Silver transformation for microsoft.calendar_event_raw
-- Reads direct structured columns from the Bronze AUTO-mode view.
--
-- NOTE: The Bronze view now exposes real ORC columns (event_id, start_json,
-- organizer_json, etc.) instead of a generic entity_data JSON blob.
--
-- JSON columns:
--   start_json / end_json  → {"dateTime": "...", "timeZone": "..."}
--   organizer_json         → {"emailAddress": {"name": "...", "address": "..."}}
--   location_json          → {"displayName": "...", "locationType": "..."}
--   response_status_json   → {"response": "...", "time": "..."}
--   attendees_json         → [{...}, ...] (exploded separately in calendar_event_attendees)
--
-- Booleans (is_all_day, is_cancelled, etc.) are Nullable(Bool) → COALESCE to false.
-- Duration is computed from start/end datetimes.
-- SETTINGS optimize_move_to_prewhere = 0 is added by the generator at the outer level.

SELECT
    -- Core event identity
    trim(assumeNotNull(event_id))                                                              AS event_id,
    trim(assumeNotNull(i_cal_uid))                                                             AS icalendar_uid,
    trim(assumeNotNull(web_link))                                                              AS event_web_link,

    -- Calendar reference
    trim(assumeNotNull(calendar_id))                                                           AS calendar_id,
    trim(assumeNotNull(calendar_name))                                                         AS calendar_name,
    lower(trim(assumeNotNull(calendar_owner_email)))                                           AS calendar_owner_email,
    trim(assumeNotNull(user_id))                                                               AS user_id,

    -- Event content
    trim(assumeNotNull(subject))                                                               AS subject,
    trim(assumeNotNull(body_preview))                                                          AS body_preview,
    JSONExtractString(assumeNotNull(body_json), 'contentType')                                 AS body_content_type,

    -- Timing (extracted from start_json / end_json)
    parseDateTime64BestEffortOrNull(
        JSONExtractString(assumeNotNull(start_json), 'dateTime')
    )                                                                                          AS start_datetime,
    parseDateTime64BestEffortOrNull(
        JSONExtractString(assumeNotNull(end_json), 'dateTime')
    )                                                                                          AS end_datetime,
    JSONExtractString(assumeNotNull(start_json), 'timeZone')                                   AS start_timezone,
    JSONExtractString(assumeNotNull(end_json),   'timeZone')                                   AS end_timezone,
    coalesce(is_all_day, false)                                                                AS is_all_day,
    if(
        start_json IS NOT NULL AND end_json IS NOT NULL,
        toInt32(dateDiff('minute',
            assumeNotNull(parseDateTime64BestEffortOrNull(JSONExtractString(assumeNotNull(start_json), 'dateTime'))),
            assumeNotNull(parseDateTime64BestEffortOrNull(JSONExtractString(assumeNotNull(end_json),   'dateTime')))
        )),
        0
    )                                                                                          AS duration_minutes,

    -- Location (from location_json)
    JSONExtractString(assumeNotNull(location_json), 'displayName')                             AS location,
    JSONExtractString(assumeNotNull(location_json), 'displayName')                             AS location_display_name,
    JSONExtractString(assumeNotNull(location_json), 'locationType')                            AS location_type,

    -- Organizer (from organizer_json)
    lower(trim(JSONExtractString(assumeNotNull(organizer_json), 'emailAddress', 'address')))   AS organizer_email,
    trim(JSONExtractString(assumeNotNull(organizer_json), 'emailAddress', 'name'))             AS organizer_name,

    -- Attendee counts (from attendees_json array)
    if(attendees_json IS NULL OR attendees_json = '' OR attendees_json = '[]',
        0,
        toInt32(length(JSONExtractArrayRaw(assumeNotNull(attendees_json))))
    )                                                                                          AS attendee_count,
    if(attendees_json IS NULL OR attendees_json = '' OR attendees_json = '[]',
        0,
        toInt32(arrayCount(x -> lower(JSONExtractString(x, 'type')) = 'required',
            JSONExtractArrayRaw(assumeNotNull(attendees_json))))
    )                                                                                          AS required_attendee_count,
    if(attendees_json IS NULL OR attendees_json = '' OR attendees_json = '[]',
        0,
        toInt32(arrayCount(x -> lower(JSONExtractString(x, 'type')) = 'optional',
            JSONExtractArrayRaw(assumeNotNull(attendees_json))))
    )                                                                                          AS optional_attendee_count,
    if(attendees_json IS NULL OR attendees_json = '' OR attendees_json = '[]',
        0,
        toInt32(arrayCount(x -> lower(JSONExtractString(x, 'type')) = 'resource',
            JSONExtractArrayRaw(assumeNotNull(attendees_json))))
    )                                                                                          AS resource_count,

    -- Online meeting (direct columns)
    coalesce(is_online_meeting, false)                                                         AS is_online_meeting,
    trim(assumeNotNull(online_meeting_provider))                                               AS online_meeting_provider,
    trim(assumeNotNull(online_meeting_url))                                                    AS online_meeting_url,

    -- Status / flags (direct Nullable(Bool) → COALESCE to false)
    trim(assumeNotNull(show_as))                                                               AS show_as,
    trim(assumeNotNull(sensitivity))                                                           AS sensitivity,
    trim(assumeNotNull(importance))                                                            AS importance,
    coalesce(is_cancelled,  false)                                                             AS is_cancelled,
    coalesce(is_draft,      false)                                                             AS is_draft,
    coalesce(is_organizer,  false)                                                             AS is_organizer,
    coalesce(is_recurring,  false)                                                             AS is_recurring,

    -- Attachments / categories
    coalesce(has_attachments, false)                                                           AS has_attachments,
    if(attachments_json IS NULL OR attachments_json = '' OR attachments_json = '[]',
        0,
        toInt32(length(JSONExtractArrayRaw(assumeNotNull(attachments_json))))
    )                                                                                          AS attachment_count,
    if(categories_json IS NULL OR categories_json = '' OR categories_json = '[]',
        0,
        toInt32(length(JSONExtractArrayRaw(assumeNotNull(categories_json))))
    )                                                                                          AS category_count,

    -- Response status (from response_status_json → {"response": "accepted", "time": "..."})
    lower(trim(JSONExtractString(assumeNotNull(response_status_json), 'response')))            AS response_status,

    -- Audit timestamps (direct string columns → parse to DateTime64)
    parseDateTime64BestEffortOrNull(assumeNotNull(created_date_time))                          AS created_datetime,
    parseDateTime64BestEffortOrNull(assumeNotNull(last_modified_date_time))                    AS last_modified_datetime,

    -- Processing metadata (partition columns)
    glynac_organization_id,
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp))                            AS processing_timestamp,

    -- System columns
    now()                          AS _loaded_at,
    'microsoft.calendar_event_raw' AS _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp))                            AS _source_timestamp

FROM microsoft.calendar_event_raw
WHERE event_id IS NOT NULL
  AND event_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY event_id, glynac_organization_id, processing_date
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1
