SELECT
    -- Core email identifier
    trimBoth(COALESCE(email_id, ''))               AS email_id,
    trimBoth(COALESCE(glynac_organization_id, '')) AS glynac_organization_id,

    -- Mailbox owner
    trimBoth(COALESCE(user_upn, ''))               AS user_upn,

    -- Email content
    trimBoth(COALESCE(subject, ''))                AS subject,
    trimBoth(COALESCE(body_preview, ''))           AS body_preview,
    trimBoth(COALESCE(importance, ''))             AS importance,
    trimBoth(COALESCE(folder_name, ''))            AS folder_name,

    -- Date/time fields
    parseDateTime64BestEffortOrNull(toString(received_date_time)) AS received_date_time,
    parseDateTime64BestEffortOrNull(toString(sent_date_time))     AS sent_date_time,

    -- Flags (Nullable(Bool) in Bronze — coalesce to false)
    COALESCE(has_attachments, false) AS has_attachments,
    COALESCE(is_draft, false)        AS is_draft,
    COALESCE(is_read, false)         AS is_read,

    -- Sender / recipients
    trimBoth(COALESCE(from_address, ''))           AS from_address,
    trimBoth(COALESCE(to_recipients_json, ''))     AS to_recipients_json,
    trimBoth(COALESCE(cc_recipients_json, ''))     AS cc_recipients_json,

    -- Processing metadata
    processing_date,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) AS processing_timestamp,

    -- System columns
    now()                              AS _loaded_at,
    'microsoft.email_raw'              AS _source_table,
    parseDateTime64BestEffortOrNull(toString(processing_timestamp)) AS _source_timestamp

FROM microsoft.email_raw
WHERE email_id IS NOT NULL
  AND email_id != ''
  AND glynac_organization_id IS NOT NULL
  AND glynac_organization_id != ''
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY email_id, glynac_organization_id, processing_date
    ORDER BY parseDateTime64BestEffortOrNull(toString(processing_timestamp)) DESC
) = 1