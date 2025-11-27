-- Redtail Client Data Processing
-- Source: Read from Kafka with ARRAY<ROW> schema

CREATE TABLE redtail_client_raw_input (
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
        contact_id INT,
        client_number STRING,
        client_type STRING,
        status STRING,
        service_level STRING,
        risk_profile STRING,
        assets_under_management STRING,
        onboarding_date STRING,
        review_date STRING,
        notes STRING,
        rec_id INT,
        rec_add STRING,
        rec_edit STRING,
        rec_add_user INT,
        rec_edit_user INT,
        id INT
    )>,
    processing_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'redtail-communications-stream',
    'properties.bootstrap.servers' = '{{KAFKA_BOOTSTRAP_SERVERS}}',
    'properties.group.id' = 'redtail-client-orc-processor',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- Sink: Write to MinIO as ORC
CREATE TABLE redtail_client_storage (
    scan_id STRING,
    glynac_organization_id STRING,
    entity_type STRING,
    batch_timestamp STRING,
    batch_number INT,
    batch_offset BIGINT,
    message_type STRING,
    batch_status STRING,
    client_id INT,
    contact_id INT,
    client_number STRING,
    client_type STRING,
    client_status STRING,
    service_level STRING,
    risk_profile STRING,
    assets_under_management STRING,
    onboarding_date STRING,
    review_date STRING,
    notes STRING,
    rec_id INT,
    rec_add STRING,
    rec_edit STRING,
    rec_add_user INT,
    rec_edit_user INT,
    processing_timestamp STRING,
    processing_date STRING
) PARTITIONED BY (glynac_organization_id, processing_date)
WITH (
    'connector' = 'filesystem',
    'path' = 's3a://main-data/redtail/client/',
    'format' = 'orc',
    'sink.rolling-policy.rollover-interval' = '1day',
    'sink.rolling-policy.check-interval' = '1h'
);

-- UNNEST and write
INSERT INTO redtail_client_storage
SELECT 
    COALESCE(scan_id, 'UNKNOWN') as scan_id,
    COALESCE(glynac_organization_id, 'unknown') as glynac_organization_id,
    COALESCE(entity_type, 'client') as entity_type,
    COALESCE(`timestamp`, CAST(CURRENT_TIMESTAMP AS STRING)) as batch_timestamp,
    COALESCE(batch_number, 0) as batch_number,
    COALESCE(batch_offset, 0) as batch_offset,
    COALESCE(message_type, 'batch_data') as message_type,
    COALESCE(redtail_client_raw_input.status, 'unknown') as batch_status,
    COALESCE(client.id, 0) as client_id,
    COALESCE(client.contact_id, 0) as contact_id,
    COALESCE(client.client_number, '') as client_number,
    COALESCE(client.client_type, 'Unknown') as client_type,
    COALESCE(client.status, 'Unknown') as client_status,
    COALESCE(client.service_level, '') as service_level,
    COALESCE(client.risk_profile, '') as risk_profile,
    COALESCE(client.assets_under_management, '0.00') as assets_under_management,
    COALESCE(client.onboarding_date, '') as onboarding_date,
    COALESCE(client.review_date, '') as review_date,
    COALESCE(client.notes, '') as notes,
    COALESCE(client.rec_id, 0) as rec_id,
    COALESCE(client.rec_add, '') as rec_add,
    COALESCE(client.rec_edit, '') as rec_edit,
    COALESCE(client.rec_add_user, 0) as rec_add_user,
    COALESCE(client.rec_edit_user, 0) as rec_edit_user,
    CAST(processing_time AS STRING) as processing_timestamp,
    DATE_FORMAT(processing_time, 'yyyy-MM-dd') as processing_date
FROM redtail_client_raw_input
CROSS JOIN UNNEST(records) AS client;
