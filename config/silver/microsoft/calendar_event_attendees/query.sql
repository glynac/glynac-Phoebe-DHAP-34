-- =============================================================================
-- Silver: microsoft_silver.calendar_event_attendees
-- =============================================================================
-- Purpose:
--   Normalize the attendees_json array from microsoft.calendar_event_raw into
--   one row per attendee per calendar event.
--
-- Source:  microsoft.calendar_event_raw
-- Target:  microsoft_silver.calendar_event_attendees
--
-- Normalization:
--   attendees_json = '[{"type":"required","status":{"response":"accepted","time":"..."},
--                       "emailAddress":{"name":"...","address":"..."}}, ...]'
--   Each element in the array becomes one row using arrayJoin(JSONExtractArrayRaw(...))
--
-- Deduplication (QUALIFY):
--   Latest record per (event_id, attendee_email, glynac_organization_id)
--   using processing_date DESC — handles re-scans and API updates
--
-- Date filter injection:
--   The DAG generator appends:
--     AND processing_date = toDate('YYYY-MM-DD')
--   after the QUALIFY clause automatically.
--
-- Known values for response_status:
--   none | accepted | declined | notresponded | tentativelyaccepted | organizer
--
-- NOTE: assumeNotNull(attendees_json) is required because attendees_json is
--   Nullable(String) in Bronze — JSONExtractArrayRaw cannot work with Nullable.
-- =============================================================================

WITH base AS (
    SELECT
        event_id,
        calendar_id,
        calendar_owner_email,
        user_id,
        subject,
        glynac_organization_id,
        processing_date,
        arrayJoin(
            JSONExtractArrayRaw(assumeNotNull(attendees_json))
        ) AS attendee_raw
    FROM microsoft.calendar_event_raw
    WHERE attendees_json IS NOT NULL
      AND attendees_json != '[]'
      AND attendees_json != ''
)
SELECT
    -- Event context
    event_id,
    calendar_id,
    lower(trim(calendar_owner_email))                                         AS calendar_owner_email,
    lower(trim(user_id))                                                      AS calendar_owner_upn,
    subject,

    -- Attendee identity
    lower(trim(JSONExtractString(attendee_raw, 'type')))                      AS attendee_type,
    trim(JSONExtractString(attendee_raw, 'emailAddress', 'name'))             AS attendee_name,
    lower(trim(JSONExtractString(attendee_raw, 'emailAddress', 'address')))   AS attendee_email,
    lower(trim(
        splitByChar('@',
            lower(trim(JSONExtractString(attendee_raw, 'emailAddress', 'address')))
        )[-1]
    ))                                                                         AS attendee_domain,

    -- RSVP / response
    lower(trim(JSONExtractString(attendee_raw, 'status', 'response')))        AS response_status,
    JSONExtractString(attendee_raw, 'status', 'time')                         AS response_time,

    -- Partition keys (must be last, match schema.yaml column order)
    glynac_organization_id,
    processing_date

FROM base
WHERE JSONExtractString(attendee_raw, 'emailAddress', 'address') != ''

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
        event_id,
        lower(trim(JSONExtractString(attendee_raw, 'emailAddress', 'address'))),
        glynac_organization_id
    ORDER BY processing_date DESC
) = 1

SETTINGS optimize_move_to_prewhere = 0
