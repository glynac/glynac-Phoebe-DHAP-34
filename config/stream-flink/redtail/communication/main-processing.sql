-- Redtail Communication Data Processing
-- Source: Read from Kafka with ARRAY<ROW> schema

CREATE TABLE redtail_communication_raw_input (
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
        communication_type STRING,
        direction STRING,
        subject STRING,
        body STRING,
        contact_id INT,
        from_user_id INT,
        to_contact_id INT,
        status STRING,
        read_status BOOLEAN,
        communication_date STRING,
        attachments_count INT,
        rec_add STRING,
        rec_edit STRING,
        id INT,
        notes STRING
    )>,
    processing_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'redtail-communications-stream',
    'properties.bootstrap.servers' = '{{KAFKA_BOOTSTRAP_SERVERS}}',
    'properties.group.id' = 'redtail-communication-orc-processor',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- Sink: Write to MinIO as ORC
CREATE TABLE redtail_communication_storage (
    scan_id STRING,
    glynac_organization_id STRING,
    entity_type STRING,
    batch_timestamp STRING,
    batch_number INT,
    batch_offset BIGINT,
    message_type STRING,
    rec_id INT,
    communication_id INT,
    communication_type STRING,
    direction STRING,
    subject STRING,
    body STRING,
    contact_id INT,
    from_user_id INT,
    to_contact_id INT,
    comm_status STRING,
    read_status BOOLEAN,
    communication_date STRING,
    attachments_count INT,
    notes STRING,
    rec_add STRING,
    rec_edit STRING,
    processing_timestamp STRING,
    processing_date STRING
) PARTITIONED BY (glynac_organization_id, processing_date)
WITH (
    'connector' = 'filesystem',
    'path' = 's3a://main-data/redtail/communication/',
    'format' = 'orc',
    'sink.rolling-policy.rollover-interval' = '1day',
    'sink.rolling-policy.check-interval' = '1h'
);

-- UNNEST and write
INSERT INTO redtail_communication_storage
SELECT 
    COALESCE(scan_id, 'UNKNOWN') as scan_id,
    COALESCE(glynac_organization_id, 'unknown') as glynac_organization_id,
    COALESCE(entity_type, 'communication') as entity_type,
    COALESCE(`timestamp`, CAST(CURRENT_TIMESTAMP AS STRING)) as batch_timestamp,
    COALESCE(batch_number, 0) as batch_number,
    COALESCE(batch_offset, 0) as batch_offset,
    COALESCE(message_type, 'batch_data') as message_type,
    COALESCE(comm.rec_id, 0) as rec_id,
    COALESCE(comm.id, 0) as communication_id,
    COALESCE(comm.communication_type, 'Unknown') as communication_type,
    COALESCE(comm.direction, 'Unknown') as direction,
    COALESCE(comm.subject, '') as subject,
    COALESCE(comm.body, '') as body,
    COALESCE(comm.contact_id, 0) as contact_id,
    COALESCE(comm.from_user_id, 0) as from_user_id,
    COALESCE(comm.to_contact_id, 0) as to_contact_id,
    COALESCE(comm.status, 'Unknown') as comm_status,
    COALESCE(comm.read_status, false) as read_status,
    COALESCE(comm.communication_date, '') as communication_date,
    COALESCE(comm.attachments_count, 0) as attachments_count,
    COALESCE(comm.notes, '') as notes,
    COALESCE(comm.rec_add, '') as rec_add,
    COALESCE(comm.rec_edit, '') as rec_edit,
    CAST(processing_time AS STRING) as processing_timestamp,
    DATE_FORMAT(processing_time, 'yyyy-MM-dd') as processing_date
FROM redtail_communication_raw_input
CROSS JOIN UNNEST(records) AS comm;
