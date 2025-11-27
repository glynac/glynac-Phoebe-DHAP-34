-- Redtail Call Data Processing
-- Source: Read from Kafka with ARRAY<ROW> schema

CREATE TABLE redtail_call_raw_input (
    scan_id STRING,
    entity_type STRING,
    glynac_organization_id STRING,
    batch_offset BIGINT,
    batch_size INT,
    batch_number INT,
    `timestamp` STRING,
    status STRING,
    message_type STRING,
    records ARRAY<ROW(
        rec_id INT,
        call_type STRING,
        call_status STRING,
        phone_number STRING,
        contact_id INT,
        user_id INT,
        call_date STRING,
        call_time STRING,
        duration_seconds INT,
        duration_minutes INT,
        subject STRING,
        notes STRING,
        disposition STRING,
        follow_up_required BOOLEAN,
        follow_up_date STRING,
        has_recording BOOLEAN,
        rec_add STRING,
        rec_edit STRING,
        id INT,
        outcome STRING,
        recording_url STRING
    )>,
    processing_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'redtail-calls-stream',
    'properties.bootstrap.servers' = '{{KAFKA_BOOTSTRAP_SERVERS}}',
    'properties.group.id' = 'redtail-call-orc-processor',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- Sink: Write to MinIO as ORC
CREATE TABLE redtail_call_storage (
    scan_id STRING,
    glynac_organization_id STRING,
    entity_type STRING,
    batch_timestamp STRING,
    batch_number INT,
    batch_size INT,
    message_status STRING,
    call_id INT,
    rec_id INT,
    call_type STRING,
    call_status STRING,
    phone_number STRING,
    contact_id INT,
    user_id INT,
    call_date STRING,
    call_time STRING,
    duration_seconds INT,
    duration_minutes INT,
    subject STRING,
    notes STRING,
    disposition STRING,
    outcome STRING,
    follow_up_required BOOLEAN,
    follow_up_date STRING,
    has_recording BOOLEAN,
    recording_url STRING,
    rec_add STRING,
    rec_edit STRING,
    processing_timestamp STRING,
    processing_date STRING
) PARTITIONED BY (glynac_organization_id, processing_date)
WITH (
    'connector' = 'filesystem',
    'path' = 's3a://main-data/redtail/call/',
    'format' = 'orc',
    'sink.rolling-policy.rollover-interval' = '1day',
    'sink.rolling-policy.check-interval' = '1h'
);

-- UNNEST and write
INSERT INTO redtail_call_storage
SELECT 
    COALESCE(scan_id, 'UNKNOWN') as scan_id,
    COALESCE(glynac_organization_id, 'unknown') as glynac_organization_id,
    COALESCE(entity_type, 'call') as entity_type,
    COALESCE(`timestamp`, CAST(CURRENT_TIMESTAMP AS STRING)) as batch_timestamp,
    COALESCE(batch_number, 0) as batch_number,
    COALESCE(batch_size, 0) as batch_size,
    COALESCE(status, 'unknown') as message_status,
    COALESCE(call_rec.id, 0) as call_id,
    COALESCE(call_rec.rec_id, 0) as rec_id,
    COALESCE(call_rec.call_type, 'Unknown') as call_type,
    COALESCE(call_rec.call_status, 'Unknown') as call_status,
    COALESCE(call_rec.phone_number, '') as phone_number,
    COALESCE(call_rec.contact_id, 0) as contact_id,
    COALESCE(call_rec.user_id, 0) as user_id,
    COALESCE(call_rec.call_date, '') as call_date,
    COALESCE(call_rec.call_time, '') as call_time,
    COALESCE(call_rec.duration_seconds, 0) as duration_seconds,
    COALESCE(call_rec.duration_minutes, 0) as duration_minutes,
    COALESCE(call_rec.subject, '') as subject,
    COALESCE(call_rec.notes, '') as notes,
    COALESCE(call_rec.disposition, '') as disposition,
    COALESCE(call_rec.outcome, '') as outcome,
    COALESCE(call_rec.follow_up_required, false) as follow_up_required,
    COALESCE(call_rec.follow_up_date, '') as follow_up_date,
    COALESCE(call_rec.has_recording, false) as has_recording,
    COALESCE(call_rec.recording_url, '') as recording_url,
    COALESCE(call_rec.rec_add, '') as rec_add,
    COALESCE(call_rec.rec_edit, '') as rec_edit,
    CAST(processing_time AS STRING) as processing_timestamp,
    DATE_FORMAT(processing_time, 'yyyy-MM-dd') as processing_date
FROM redtail_call_raw_input
CROSS JOIN UNNEST(records) AS call_rec;
