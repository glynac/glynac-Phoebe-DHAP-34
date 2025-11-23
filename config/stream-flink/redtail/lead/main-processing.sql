-- Redtail Lead Data Processing
-- Source: Read from Kafka with ARRAY<ROW> schema

CREATE TABLE redtail_lead_raw_input (
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
        first_name STRING,
        last_name STRING,
        full_name STRING,
        email STRING,
        phone_number STRING,
        company_name STRING,
        status STRING,
        source STRING,
        rating STRING,
        assigned_to INT,
        converted_to_prospect BOOLEAN,
        converted_date STRING,
        notes STRING,
        rec_id INT,
        rec_add STRING,
        rec_edit STRING,
        rec_add_user INT,
        rec_edit_user INT,
        id INT,
        job_title STRING,
        industry STRING
    )>,
    processing_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'redtail-leads-stream',
    'properties.bootstrap.servers' = '{{KAFKA_BOOTSTRAP_SERVERS}}',,
    'properties.group.id' = 'redtail-lead-orc-processor',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- Sink: Write to MinIO as ORC
CREATE TABLE redtail_lead_storage (
    scan_id STRING,
    glynac_organization_id STRING,
    entity_type STRING,
    batch_timestamp STRING,
    batch_number INT,
    batch_offset BIGINT,
    message_type STRING,
    lead_id INT,
    rec_id INT,
    first_name STRING,
    last_name STRING,
    full_name STRING,
    email STRING,
    phone_number STRING,
    company_name STRING,
    job_title STRING,
    industry STRING,
    lead_status STRING,
    source STRING,
    rating STRING,
    assigned_to INT,
    converted_to_prospect BOOLEAN,
    converted_date STRING,
    notes STRING,
    rec_add STRING,
    rec_edit STRING,
    rec_add_user INT,
    rec_edit_user INT,
    processing_timestamp STRING,
    processing_date STRING
) PARTITIONED BY (glynac_organization_id, processing_date)
WITH (
    'connector' = 'filesystem',
    'path' = 's3a://main-data/redtail/lead/',
    'format' = 'orc',
    'sink.rolling-policy.rollover-interval' = '1day',
    'sink.rolling-policy.check-interval' = '1h'
);

-- UNNEST and write
INSERT INTO redtail_lead_storage
SELECT 
    COALESCE(scan_id, 'UNKNOWN') as scan_id,
    COALESCE(glynac_organization_id, 'unknown') as glynac_organization_id,
    COALESCE(entity_type, 'lead') as entity_type,
    COALESCE(`timestamp`, CAST(CURRENT_TIMESTAMP AS STRING)) as batch_timestamp,
    COALESCE(batch_number, 0) as batch_number,
    COALESCE(batch_offset, 0) as batch_offset,
    COALESCE(message_type, 'batch_data') as message_type,
    COALESCE(lead_rec.id, 0) as lead_id,
    COALESCE(lead_rec.rec_id, 0) as rec_id,
    COALESCE(lead_rec.first_name, '') as first_name,
    COALESCE(lead_rec.last_name, '') as last_name,
    COALESCE(lead_rec.full_name, '') as full_name,
    COALESCE(lead_rec.email, '') as email,
    COALESCE(lead_rec.phone_number, '') as phone_number,
    COALESCE(lead_rec.company_name, '') as company_name,
    COALESCE(lead_rec.job_title, '') as job_title,
    COALESCE(lead_rec.industry, '') as industry,
    COALESCE(lead_rec.status, 'Unknown') as lead_status,
    COALESCE(lead_rec.source, '') as source,
    COALESCE(lead_rec.rating, '') as rating,
    COALESCE(lead_rec.assigned_to, 0) as assigned_to,
    COALESCE(lead_rec.converted_to_prospect, false) as converted_to_prospect,
    COALESCE(lead_rec.converted_date, '') as converted_date,
    COALESCE(lead_rec.notes, '') as notes,
    COALESCE(lead_rec.rec_add, '') as rec_add,
    COALESCE(lead_rec.rec_edit, '') as rec_edit,
    COALESCE(lead_rec.rec_add_user, 0) as rec_add_user,
    COALESCE(lead_rec.rec_edit_user, 0) as rec_edit_user,
    CAST(processing_time AS STRING) as processing_timestamp,
    DATE_FORMAT(processing_time, 'yyyy-MM-dd') as processing_date
FROM redtail_lead_raw_input
CROSS JOIN UNNEST(records) AS lead_rec;
