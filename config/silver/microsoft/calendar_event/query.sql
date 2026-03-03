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
-- Booleans (is_all_day, is_cancelled, etc.) are Nullable(Bool) → COALESCE to false.
-- Duration is computed from start/end datetimes.
-- SETTINGS optimize_move_to_prewhere = 0 is added by the generator at the outer level.

SELECT
    -- Core event identity
    trim(coalesce(event_id, ''))                                     AS event_id,
    trim(coalesce(i_cal_uid, ''))                                    AS icalendar_uid,
    trim(coalesce(web_link, ''))                                     AS event_web_link,

    -- Calendar reference
    trim(coalesce(calendar_id, ''))                                  AS calendar_id,
    trim(coalesce(calendar_name, ''))                                AS calendar_name,
    lower(trim(coalesce(calendar_owner_email, '')))                  AS calendar_owner_email,
    trim(coalesce(user_id, ''))                                      AS user_id,

    -- Event content
    trim(coalesce(subject, ''))                                      AS subject,
    trim(coalesce(body_preview, ''))                                 AS body_preview,
    coalesce(JSONExtractString(ifNull(body_json, ''), 'contentType'), '') AS body_content_type,

    -- Timing
    parseDateTime64BestEffortOrNull(JSONExtractString(ifNull(start_json, ''), 'dateTime')) AS start_datetime,
    parseDateTime64BestEffortOrNull(JSONExtractString(ifNull(end_json, ''), 'dateTime'))   AS end_datetime,
    coalesce(JSONExtractString(ifNull(start_json, ''), 'timeZone'), '')          AS start_timezone,
    coalesce(JSONExtractString(ifNull(end_json, ''), 'timeZone'), '')            AS end_timezone,
    coalesce(is_all_day, false)                                      AS is_all_day,
    if(
        start_json IS NOT NULL AND end_json IS NOT NULL,
        toInt32(dateDiff('minute',
            assumeNotNull(parseDateTime64BestEffortOrNull(JSONExtractString(assumeNotNull(start_json), 'dateTime'))),
            assumeNotNull(parseDateTime64BestEffortOrNull(JSONExtractString(assumeNotNull(end_json), 'dateTime')))
        )),
        0
    )                                                                AS duration_minutes,

    -- Location
    coalesce(JSONExtractString(ifNull(location_json, ''), 'displayName'), '')    AS location,
    coalesce(JSONExtractString(ifNull(location_json, ''), 'displayName'), '')    AS location_display_name,
    coalesce(JSONExtractString(ifNull(location_json, ''), 'locationType'), '')   AS location_type,

    -- Organizer
    lower(trim(coalesce(JSONExtractString(ifNull(organizer_json, ''), 'emailAddress', 'address'), ''))) AS organizer_email,
    trim(coalesce(JSONExtractString(ifNull(organizer_json, ''), 'emailAddress', 'name'), ''))           AS organizer_name,

    -- Attendee counts
    toInt32(length(JSONExtractArrayRaw(ifNull(attendees_json, '[]')))) AS attendee_count,
    
    toInt32(arrayCount(x -> lower(JSONExtractString(x, 'type')) = 'required',
        JSONExtractArrayRaw(ifNull(attendees_json, '[]'))))          AS required_attendee_count,
    
    toInt32(arrayCount(x -> lower(JSONExtractString(x, 'type')) = 'optional',
        JSONExtractArrayRaw(ifNull(attendees_json, '[]'))))          AS optional_attendee_count,
    
    toInt32(arrayCount(x -> lower(JSONExtractString(x, 'type')) = 'resource',
        JSONExtractArrayRaw(ifNull(attendees_json, '[]'))))          AS resource_count,

    -- Online meeting
    coalesce(is_online_meeting, false)                               AS is_online_meeting,
    trim(coalesce(online_meeting_provider, ''))                      AS online_meeting_provider,
    trim(coalesce(online_meeting_url, ''))                           AS online_meeting_url,

    -- Status / flags
    trim(coalesce(show_as, ''))                                      AS show_as,
    trim(coalesce(sensitivity, ''))                                  AS sensitivity,
    trim(coalesce(importance, ''))                                   AS importance,
    coalesce(is_cancelled,  false)                                   AS is_cancelled,
    coalesce(is_draft,      false)                                   AS is_draft,
    coalesce(is_organizer,  false)                                   AS is_organizer,
    coalesce(is_recurring,  false)                                   AS is_recurring,

    -- Attachments / categories
    coalesce(has_attachments, false)                                 AS has_attachments,
    toInt32(length(JSONExtractArrayRaw(ifNull(attachments_json, '[]')))) AS attachment_count,
    toInt32(length(JSONExtractArrayRaw(ifNull(categories_json, '[]'))))  AS category_count,

    -- Response status
    lower(trim(coalesce(JSONExtractString(ifNull(response_status_json, ''), 'response'), ''))) AS response_status,

    -- Audit timestamps
    parseDateTime64BestEffortOrNull(ifNull(created_date_time, ''))               AS created_datetime,
    parseDateTime64BestEffortOrNull(ifNull(last_modified_date_time, ''))         AS last_modified_datetime,

    -- Processing metadata
    glynac_organization_id,
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp))  AS processing_timestamp,

    -- System columns
    now()                                                            AS _loaded_at,
    'microsoft.calendar_event_raw'                                   AS _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp))  AS _source_timestamp

FROM microsoft.calendar_event_raw
WHERE event_id IS NOT NULL AND event_id != ''
  AND glynac_organization_id IS NOT NULL AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY event_id, glynac_organization_id, processing_date
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1