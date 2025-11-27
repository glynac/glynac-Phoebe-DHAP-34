-- Redtail Prospect Data Processing
-- Source: Read from Kafka with ARRAY<ROW> schema

CREATE TABLE redtail_prospect_raw_input (
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
        contact_id INT,
        prospect_number STRING,
        status STRING,
        stage STRING,
        source STRING,
        expected_revenue STRING,
        probability INT,
        expected_close_date STRING,
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
    'topic' = 'redtail-prospects-stream',
    'properties.bootstrap.servers' = '{{KAFKA_BOOTSTRAP_SERVERS}}',
    'properties.group.id' = 'redtail-prospect-orc-processor',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- Sink: Write to MinIO as ORC
CREATE TABLE redtail_prospect_storage (
    scan_id STRING,
    glynac_organization_id STRING,
    entity_type STRING,
    batch_timestamp STRING,
    batch_number INT,
    batch_offset INT,
    batch_size INT,
    message_type STRING,
    batch_status STRING,
    prospect_id INT,
    contact_id INT,
    prospect_number STRING,
    prospect_status STRING,
    stage STRING,
    source STRING,
    expected_revenue STRING,
    probability INT,
    expected_close_date STRING,
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
    'path' = 's3a://main-data/redtail/prospect/',
    'format' = 'orc',
    'sink.rolling-policy.rollover-interval' = '1day',
    'sink.rolling-policy.check-interval' = '1h'
);

-- UNNEST and write
INSERT INTO redtail_prospect_storage
SELECT 
    COALESCE(r.scan_id, 'UNKNOWN') as scan_id,
    COALESCE(r.glynac_organization_id, 'unknown') as glynac_organization_id,
    COALESCE(r.entity_type, 'prospect') as entity_type,
    COALESCE(r.`timestamp`, CAST(CURRENT_TIMESTAMP AS STRING)) as batch_timestamp,
    COALESCE(r.batch_number, 0) as batch_number,
    COALESCE(r.batch_offset, 0) as batch_offset,
    COALESCE(r.batch_size, 0) as batch_size,
    COALESCE(r.message_type, '') as message_type,
    COALESCE(r.status, 'unknown') as batch_status,
    COALESCE(prospect.id, 0) as prospect_id,
    COALESCE(prospect.contact_id, 0) as contact_id,
    COALESCE(prospect.prospect_number, '') as prospect_number,
    COALESCE(prospect.status, 'Unknown') as prospect_status,
    COALESCE(prospect.stage, '') as stage,
    COALESCE(prospect.source, '') as source,
    COALESCE(prospect.expected_revenue, '0.00') as expected_revenue,
    COALESCE(prospect.probability, 0) as probability,
    COALESCE(prospect.expected_close_date, '') as expected_close_date,
    COALESCE(prospect.notes, '') as notes,
    COALESCE(prospect.rec_id, 0) as rec_id,
    COALESCE(prospect.rec_add, '') as rec_add,
    COALESCE(prospect.rec_edit, '') as rec_edit,
    COALESCE(prospect.rec_add_user, 0) as rec_add_user,
    COALESCE(prospect.rec_edit_user, 0) as rec_edit_user,
    CAST(r.processing_time AS STRING) as processing_timestamp,
    DATE_FORMAT(r.processing_time, 'yyyy-MM-dd') as processing_date
FROM redtail_prospect_raw_input r
CROSS JOIN UNNEST(r.records) AS prospect;
