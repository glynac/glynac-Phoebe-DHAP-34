-- Redtail Household Data Processing
-- Source: Read from Kafka with ARRAY<ROW> schema

CREATE TABLE redtail_household_raw_input (
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
        household_name STRING,
        household_number STRING,
        primary_contact_id INT,
        household_type STRING,
        status STRING,
        total_net_worth STRING,
        total_income STRING,
        address1 STRING,
        city STRING,
        state STRING,
        zip_code STRING,
        country STRING,
        rec_id INT,
        rec_add STRING,
        rec_edit STRING,
        rec_add_user INT,
        rec_edit_user INT,
        id INT,
        notes STRING,
        address2 STRING
    )>,
    processing_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'redtail-households-stream',
    'properties.bootstrap.servers' = '{{KAFKA_BOOTSTRAP_SERVERS}}',
    'properties.group.id' = 'redtail-household-orc-processor',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- Sink: Write to MinIO as ORC
CREATE TABLE redtail_household_storage (
    scan_id STRING,
    glynac_organization_id STRING,
    entity_type STRING,
    batch_timestamp STRING,
    batch_number INT,
    batch_offset BIGINT,
    message_type STRING,
    household_id INT,
    household_name STRING,
    household_number STRING,
    primary_contact_id INT,
    household_type STRING,
    household_status STRING,
    total_net_worth STRING,
    total_income STRING,
    address1 STRING,
    address2 STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    country STRING,
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
    'path' = 's3a://main-data/redtail/household/',
    'format' = 'orc',
    'sink.rolling-policy.rollover-interval' = '1day',
    'sink.rolling-policy.check-interval' = '1h'
);

-- UNNEST and write
INSERT INTO redtail_household_storage
SELECT 
    COALESCE(scan_id, 'UNKNOWN') as scan_id,
    COALESCE(glynac_organization_id, 'unknown') as glynac_organization_id,
    COALESCE(entity_type, 'household') as entity_type,
    COALESCE(`timestamp`, CAST(CURRENT_TIMESTAMP AS STRING)) as batch_timestamp,
    COALESCE(batch_number, 0) as batch_number,
    COALESCE(batch_offset, 0) as batch_offset,
    COALESCE(message_type, 'batch_data') as message_type,
    COALESCE(hh.id, 0) as household_id,
    COALESCE(hh.household_name, '') as household_name,
    COALESCE(hh.household_number, '') as household_number,
    COALESCE(hh.primary_contact_id, 0) as primary_contact_id,
    COALESCE(hh.household_type, '') as household_type,
    COALESCE(hh.status, '') as household_status,
    COALESCE(hh.total_net_worth, '0') as total_net_worth,
    COALESCE(hh.total_income, '0') as total_income,
    COALESCE(hh.address1, '') as address1,
    COALESCE(hh.address2, '') as address2,
    COALESCE(hh.city, '') as city,
    COALESCE(hh.state, '') as state,
    COALESCE(hh.zip_code, '') as zip_code,
    COALESCE(hh.country, '') as country,
    COALESCE(hh.notes, '') as notes,
    COALESCE(hh.rec_id, 0) as rec_id,
    COALESCE(hh.rec_add, '') as rec_add,
    COALESCE(hh.rec_edit, '') as rec_edit,
    COALESCE(hh.rec_add_user, 0) as rec_add_user,
    COALESCE(hh.rec_edit_user, 0) as rec_edit_user,
    CAST(processing_time AS STRING) as processing_timestamp,
    DATE_FORMAT(processing_time, 'yyyy-MM-dd') as processing_date
FROM redtail_household_raw_input
CROSS JOIN UNNEST(records) AS hh;
