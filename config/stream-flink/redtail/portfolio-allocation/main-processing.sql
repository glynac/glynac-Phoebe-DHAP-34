-- Redtail Portfolio Allocation Data Processing
-- Source: Read from Kafka with ARRAY<ROW> schema

CREATE TABLE redtail_portfolio_allocation_raw_input (
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
        rec_id BIGINT,
        portfolio_id BIGINT,
        investment_id BIGINT,
        target_percentage STRING,
        current_percentage STRING,
        min_percentage STRING,
        max_percentage STRING,
        notes STRING,
        is_core_holding BOOLEAN,
        is_active BOOLEAN,
        rec_add STRING,
        rec_add_user BIGINT,
        rec_edit STRING,
        rec_edit_user BIGINT,
        id BIGINT,
        investment_data STRING
    )>,
    processing_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'redtail-portfolio-allocations-stream',
    'properties.bootstrap.servers' = '{{KAFKA_BOOTSTRAP_SERVERS}}',,
    'properties.group.id' = 'redtail-portfolio-allocation-orc-processor',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- Sink: Write to MinIO as ORC
CREATE TABLE redtail_portfolio_allocation_storage (
    scan_id STRING,
    glynac_organization_id STRING,
    entity_type STRING,
    batch_timestamp STRING,
    batch_number INT,
    batch_offset BIGINT,
    message_type STRING,
    status STRING,
    rec_id BIGINT,
    portfolio_id BIGINT,
    investment_id BIGINT,
    target_percentage STRING,
    current_percentage STRING,
    min_percentage STRING,
    max_percentage STRING,
    notes STRING,
    is_core_holding BOOLEAN,
    is_active BOOLEAN,
    rec_add STRING,
    rec_add_user BIGINT,
    rec_edit STRING,
    rec_edit_user BIGINT,
    record_id BIGINT,
    investment_data STRING,
    processing_timestamp STRING,
    processing_date STRING
) PARTITIONED BY (glynac_organization_id, processing_date)
WITH (
    'connector' = 'filesystem',
    'path' = 's3a://main-data/redtail/portfolio-allocation/',
    'format' = 'orc',
    'sink.rolling-policy.rollover-interval' = '1day',
    'sink.rolling-policy.check-interval' = '1h'
);

-- UNNEST and write
INSERT INTO redtail_portfolio_allocation_storage
SELECT 
    COALESCE(scan_id, 'UNKNOWN') as scan_id,
    COALESCE(glynac_organization_id, 'unknown') as glynac_organization_id,
    COALESCE(entity_type, 'portfolio_allocation') as entity_type,
    COALESCE(`timestamp`, CAST(CURRENT_TIMESTAMP AS STRING)) as batch_timestamp,
    COALESCE(batch_number, 0) as batch_number,
    COALESCE(batch_offset, 0) as batch_offset,
    COALESCE(message_type, 'batch_data') as message_type,
    COALESCE(status, 'unknown') as status,
    COALESCE(alloc.rec_id, 0) as rec_id,
    COALESCE(alloc.portfolio_id, 0) as portfolio_id,
    COALESCE(alloc.investment_id, 0) as investment_id,
    COALESCE(alloc.target_percentage, '0.00') as target_percentage,
    COALESCE(alloc.current_percentage, '0.00') as current_percentage,
    COALESCE(alloc.min_percentage, '0.00') as min_percentage,
    COALESCE(alloc.max_percentage, '0.00') as max_percentage,
    COALESCE(alloc.notes, '') as notes,
    COALESCE(alloc.is_core_holding, false) as is_core_holding,
    COALESCE(alloc.is_active, false) as is_active,
    COALESCE(alloc.rec_add, '') as rec_add,
    COALESCE(alloc.rec_add_user, 0) as rec_add_user,
    COALESCE(alloc.rec_edit, '') as rec_edit,
    COALESCE(alloc.rec_edit_user, 0) as rec_edit_user,
    COALESCE(alloc.id, 0) as record_id,
    COALESCE(alloc.investment_data, '{}') as investment_data,
    CAST(processing_time AS STRING) as processing_timestamp,
    DATE_FORMAT(processing_time, 'yyyy-MM-dd') as processing_date
FROM redtail_portfolio_allocation_raw_input
CROSS JOIN UNNEST(records) AS alloc;
