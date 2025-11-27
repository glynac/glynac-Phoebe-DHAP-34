-- Redtail Email Data Processing
-- Source: Read from Kafka with ARRAY<ROW> schema

CREATE TABLE redtail_email_raw_input (
    scan_id STRING,
    entity_type STRING,
    glynac_organization_id STRING,
    batch_offset INT,
    batch_size INT,
    batch_number INT,
    `timestamp` STRING,
    status STRING,
    message_type STRING,
    records ARRAY<ROW(
        rec_id INT,
        from_address STRING,
        to_addresses STRING,
        cc_addresses STRING,
        bcc_addresses STRING,
        subject STRING,
        body_text STRING,
        body_html STRING,
        contact_id INT,
        sent_by INT,
        status STRING,
        is_read BOOLEAN,
        sent_date STRING,
        open_count INT,
        click_count INT,
        has_attachments BOOLEAN,
        attachments_count INT,
        rec_add STRING,
        rec_edit STRING,
        id INT,
        read_date STRING,
        delivered_date STRING
    )>,
    processing_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'redtail-emails-stream',
    'properties.bootstrap.servers' = '{{KAFKA_BOOTSTRAP_SERVERS}}',
    'properties.group.id' = 'redtail-email-orc-processor',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- Sink: Write to MinIO as ORC
CREATE TABLE redtail_email_storage (
    scan_id STRING,
    glynac_organization_id STRING,
    entity_type STRING,
    batch_timestamp STRING,
    batch_number INT,
    batch_offset INT,
    message_type STRING,
    email_id INT,
    rec_id INT,
    from_address STRING,
    to_addresses STRING,
    cc_addresses STRING,
    bcc_addresses STRING,
    subject STRING,
    body_text STRING,
    body_html STRING,
    contact_id INT,
    sent_by INT,
    email_status STRING,
    is_read BOOLEAN,
    sent_date STRING,
    read_date STRING,
    delivered_date STRING,
    open_count INT,
    click_count INT,
    has_attachments BOOLEAN,
    attachments_count INT,
    rec_add STRING,
    rec_edit STRING,
    processing_timestamp STRING,
    processing_date STRING
) PARTITIONED BY (glynac_organization_id, processing_date)
WITH (
    'connector' = 'filesystem',
    'path' = 's3a://main-data/redtail/email/',
    'format' = 'orc',
    'sink.rolling-policy.rollover-interval' = '1day',
    'sink.rolling-policy.check-interval' = '1h'
);

-- UNNEST and write
INSERT INTO redtail_email_storage
SELECT 
    COALESCE(scan_id, 'UNKNOWN') as scan_id,
    COALESCE(glynac_organization_id, 'unknown') as glynac_organization_id,
    COALESCE(entity_type, 'email') as entity_type,
    COALESCE(`timestamp`, CAST(CURRENT_TIMESTAMP AS STRING)) as batch_timestamp,
    COALESCE(batch_number, 0) as batch_number,
    COALESCE(batch_offset, 0) as batch_offset,
    COALESCE(message_type, 'batch_data') as message_type,
    COALESCE(email.id, 0) as email_id,
    COALESCE(email.rec_id, 0) as rec_id,
    COALESCE(email.from_address, '') as from_address,
    COALESCE(email.to_addresses, '') as to_addresses,
    COALESCE(email.cc_addresses, '') as cc_addresses,
    COALESCE(email.bcc_addresses, '') as bcc_addresses,
    COALESCE(email.subject, '') as subject,
    COALESCE(email.body_text, '') as body_text,
    COALESCE(email.body_html, '') as body_html,
    COALESCE(email.contact_id, 0) as contact_id,
    COALESCE(email.sent_by, 0) as sent_by,
    COALESCE(email.status, 'Unknown') as email_status,
    COALESCE(email.is_read, false) as is_read,
    COALESCE(email.sent_date, '') as sent_date,
    COALESCE(email.read_date, '') as read_date,
    COALESCE(email.delivered_date, '') as delivered_date,
    COALESCE(email.open_count, 0) as open_count,
    COALESCE(email.click_count, 0) as click_count,
    COALESCE(email.has_attachments, false) as has_attachments,
    COALESCE(email.attachments_count, 0) as attachments_count,
    COALESCE(email.rec_add, '') as rec_add,
    COALESCE(email.rec_edit, '') as rec_edit,
    CAST(processing_time AS STRING) as processing_timestamp,
    DATE_FORMAT(processing_time, 'yyyy-MM-dd') as processing_date
FROM redtail_email_raw_input
CROSS JOIN UNNEST(records) AS email;
