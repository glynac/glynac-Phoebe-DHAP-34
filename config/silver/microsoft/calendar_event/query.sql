-- Silver Layer - Microsoft Calendar Event Transformation
-- Extracts 40+ fields from entity_data JSON and applies transformations
-- Source: microsoft.calendar_event_raw

SELECT
    -- Core record identifiers
    record_id,
    glynac_organization_id,
    tenant_id,
    is_active,

    -- Extract core event identifiers from JSON
    trimBoth(COALESCE(JSONExtractString(entity_data, 'event_id'), '')) as event_id,
    trimBoth(COALESCE(JSONExtractString(entity_data, 'icalendar_uid'), '')) as icalendar_uid,
    trimBoth(COALESCE(JSONExtractString(entity_data, 'calendar_id'), '')) as calendar_id,
    trimBoth(COALESCE(JSONExtractString(entity_data, 'calendar_name'), '')) as calendar_name,

    -- Calendar owner info
    multiIf(
        JSONExtractString(entity_data, 'calendar_owner_email') LIKE '%@%',
        toLower(trimBoth(JSONExtractString(entity_data, 'calendar_owner_email'))),
        ''
    ) as calendar_owner_email,
    JSONExtractInt(entity_data, 'calendar_owner_id') as calendar_owner_id,

    -- Event details
    trimBoth(COALESCE(JSONExtractString(entity_data, 'subject'), '')) as subject,
    trimBoth(COALESCE(JSONExtractString(entity_data, 'body_preview'), '')) as body_preview,
    trimBoth(COALESCE(JSONExtractString(entity_data, 'body_content_type'), '')) as body_content_type,

    -- DateTime fields - parse ISO8601 timestamps
    parseDateTime64BestEffortOrNull(JSONExtractString(entity_data, 'start_datetime')) as start_datetime,
    parseDateTime64BestEffortOrNull(JSONExtractString(entity_data, 'end_datetime')) as end_datetime,
    trimBoth(COALESCE(JSONExtractString(entity_data, 'start_timezone'), '')) as start_timezone,
    trimBoth(COALESCE(JSONExtractString(entity_data, 'end_timezone'), '')) as end_timezone,

    -- Event properties
    COALESCE(JSONExtractBool(entity_data, 'is_all_day'), false) as is_all_day,
    COALESCE(JSONExtractInt(entity_data, 'duration_minutes'), 0) as duration_minutes,
    trimBoth(COALESCE(JSONExtractString(entity_data, 'location'), '')) as location,
    trimBoth(COALESCE(JSONExtractString(entity_data, 'location_display_name'), '')) as location_display_name,
    trimBoth(COALESCE(JSONExtractString(entity_data, 'location_type'), '')) as location_type,

    -- Organizer information
    multiIf(
        JSONExtractString(entity_data, 'organizer_email') LIKE '%@%',
        toLower(trimBoth(JSONExtractString(entity_data, 'organizer_email'))),
        ''
    ) as organizer_email,
    trimBoth(COALESCE(JSONExtractString(entity_data, 'organizer_name'), '')) as organizer_name,
    JSONExtractInt(entity_data, 'organizer_id') as organizer_id,

    -- Attendee counts
    COALESCE(JSONExtractInt(entity_data, 'attendee_count'), 0) as attendee_count,
    COALESCE(JSONExtractInt(entity_data, 'required_attendee_count'), 0) as required_attendee_count,
    COALESCE(JSONExtractInt(entity_data, 'optional_attendee_count'), 0) as optional_attendee_count,
    COALESCE(JSONExtractInt(entity_data, 'resource_count'), 0) as resource_count,

    -- Meeting properties
    COALESCE(JSONExtractBool(entity_data, 'is_online_meeting'), false) as is_online_meeting,
    trimBoth(COALESCE(JSONExtractString(entity_data, 'online_meeting_provider'), '')) as online_meeting_provider,
    trimBoth(COALESCE(JSONExtractString(entity_data, 'online_meeting_url'), '')) as online_meeting_url,

    -- Event status and flags
    trimBoth(COALESCE(JSONExtractString(entity_data, 'show_as'), '')) as show_as,
    trimBoth(COALESCE(JSONExtractString(entity_data, 'sensitivity'), '')) as sensitivity,
    trimBoth(COALESCE(JSONExtractString(entity_data, 'importance'), '')) as importance,
    COALESCE(JSONExtractBool(entity_data, 'is_cancelled'), false) as is_cancelled,
    COALESCE(JSONExtractBool(entity_data, 'is_draft'), false) as is_draft,
    COALESCE(JSONExtractBool(entity_data, 'is_organizer'), false) as is_organizer,

    -- Recurring event properties
    COALESCE(JSONExtractBool(entity_data, 'is_recurring'), false) as is_recurring,
    trimBoth(COALESCE(JSONExtractString(entity_data, 'series_master_id'), '')) as series_master_id,
    COALESCE(JSONExtractBool(entity_data, 'is_exception'), false) as is_exception,

    -- Attachments and categories
    COALESCE(JSONExtractBool(entity_data, 'has_attachments'), false) as has_attachments,
    COALESCE(JSONExtractInt(entity_data, 'attachment_count'), 0) as attachment_count,
    COALESCE(JSONExtractInt(entity_data, 'category_count'), 0) as category_count,

    -- Response status
    trimBoth(COALESCE(JSONExtractString(entity_data, 'response_status'), '')) as response_status,

    -- Links and metadata
    trimBoth(COALESCE(JSONExtractString(entity_data, 'web_link'), '')) as web_link,
    parseDateTime64BestEffortOrNull(JSONExtractString(entity_data, 'created_datetime')) as created_datetime,
    parseDateTime64BestEffortOrNull(JSONExtractString(entity_data, 'last_modified_datetime')) as last_modified_datetime,

    -- Bronze metadata fields (transformed)
    parseDateTime64BestEffortOrNull(pii_masked_at) as pii_masked_datetime,

    -- Scan metadata
    scan_id,
    batch_offset,
    batch_size,
    batch_number,
    parseDateTime64BestEffortOrNull(scan_timestamp) as scan_datetime,
    batch_status,
    message_type,

    -- Pagination metadata
    pagination_total,
    pagination_offset,
    pagination_limit,
    pagination_has_more,

    -- Processing metadata
    parseDateTime64BestEffortOrNull(processing_timestamp) as processing_datetime,
    processing_date,

    -- System audit columns
    now() as _loaded_at,
    'microsoft.calendar_event_raw' as _source_table,
    parseDateTime64BestEffortOrNull(processing_timestamp) as _source_timestamp

FROM microsoft.calendar_event_raw

-- Apply filters
WHERE record_id IS NOT NULL
  AND record_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
  AND tenant_id IS NOT NULL
  AND tenant_id != ''
  AND entity_data IS NOT NULL
  AND isValidJSON(entity_data) = 1

-- Deduplication: Keep latest record per record_id and processing_date
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY record_id, processing_date
    ORDER BY parseDateTime64BestEffortOrNull(processing_timestamp) DESC,
             parseDateTime64BestEffortOrNull(scan_timestamp) DESC
) = 1
