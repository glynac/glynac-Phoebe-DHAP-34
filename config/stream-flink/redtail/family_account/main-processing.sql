-- Redtail Family Account Data Processing
-- Source: Read from Kafka with ARRAY<ROW> schema

CREATE TABLE redtail_family_account_raw_input (
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
        family_name STRING,
        account_number STRING,
        status STRING,
        total_value STRING,
        rec_id INT,
        rec_add STRING,
        rec_edit STRING,
        rec_add_user INT,
        rec_edit_user INT,
        id INT,
        notes STRING
    )>,
    processing_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'redtail-leads-stream',
    'properties.bootstrap.servers' = '{{KAFKA_BOOTSTRAP_SERVERS}}',
    'properties.group.id' = 'redtail-family-account-orc-processor',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- Sink: Write to MinIO as ORC
CREATE TABLE redtail_family_account_storage (
    scan_id STRING,
    glynac_organization_id STRING,
    entity_type STRING,
    batch_timestamp STRING,
    batch_number INT,
    batch_offset BIGINT,
    message_type STRING,
    batch_status STRING,
    family_id INT,
    family_name STRING,
    account_number STRING,
    account_status STRING,
    total_value STRING,
    rec_id INT,
    rec_add STRING,
    rec_edit STRING,
    rec_add_user INT,
    rec_edit_user INT,
    notes STRING,
    processing_timestamp STRING,
    processing_date STRING
) PARTITIONED BY (glynac_organization_id, processing_date)
WITH (
    'connector' = 'filesystem',
    'path' = 's3a://main-data/redtail/family_account/',
    'format' = 'orc',
    'sink.rolling-policy.rollover-interval' = '1day',
    'sink.rolling-policy.check-interval' = '1h'
);

-- UNNEST and write
INSERT INTO redtail_family_account_storage
SELECT 
    COALESCE(scan_id, 'UNKNOWN') as scan_id,
    COALESCE(glynac_organization_id, 'unknown') as glynac_organization_id,
    COALESCE(entity_type, 'family_account') as entity_type,
    COALESCE(`timestamp`, CAST(CURRENT_TIMESTAMP AS STRING)) as batch_timestamp,
    COALESCE(batch_number, 0) as batch_number,
    COALESCE(batch_offset, 0) as batch_offset,
    COALESCE(message_type, 'batch_data') as message_type,
    COALESCE(redtail_family_account_raw_input.status, 'unknown') as batch_status,
    COALESCE(record.id, 0) as family_id,
    COALESCE(record.family_name, '') as family_name,
    COALESCE(record.account_number, '') as account_number,
    COALESCE(record.status, 'Unknown') as account_status,
    COALESCE(record.total_value, '0') as total_value,
    COALESCE(record.rec_id, 0) as rec_id,
    COALESCE(record.rec_add, '') as rec_add,
    COALESCE(record.rec_edit, '') as rec_edit,
    COALESCE(record.rec_add_user, 0) as rec_add_user,
    COALESCE(record.rec_edit_user, 0) as rec_edit_user,
    COALESCE(record.notes, '') as notes,
    CAST(processing_time AS STRING) as processing_timestamp,
    DATE_FORMAT(processing_time, 'yyyy-MM-dd') as processing_date
FROM redtail_family_account_raw_input
CROSS JOIN UNNEST(records) AS record;
