-- Redtail Activity Data Processing
-- Source: Read from Kafka with ARRAY<ROW> schema

CREATE TABLE redtail_activity_raw_input (
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
        activity_type STRING,
        category STRING,
        subject STRING,
        description STRING,
        contact_id INT,
        owner_id INT,
        status STRING,
        priority STRING,
        activity_date STRING,
        follow_up_required BOOLEAN,
        rec_add STRING,
        rec_edit STRING,
        id INT,
        subcategory STRING,
        duration_minutes INT,
        outcome STRING,
        location STRING,
        tags STRING,
        follow_up_date STRING,
        notes STRING
    )>,
    processing_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'redtail-activities-stream',
    'properties.bootstrap.servers' = '{{KAFKA_BOOTSTRAP_SERVERS}}',
    'properties.group.id' = 'redtail-activity-orc-processor',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);


-- Sink: Write to MinIO as ORC
CREATE TABLE redtail_activity_storage (
    scan_id STRING,
    glynac_organization_id STRING,
    entity_type STRING,
    batch_timestamp STRING,
    batch_number INT,
    batch_offset BIGINT,
    message_type STRING,
    batch_status STRING,
    activity_id INT,
    rec_id INT,
    activity_type STRING,
    category STRING,
    subcategory STRING,
    subject STRING,
    description STRING,
    contact_id INT,
    owner_id INT,
    activity_status STRING,
    priority STRING,
    activity_date STRING,
    duration_minutes INT,
    outcome STRING,
    location STRING,
    tags STRING,
    follow_up_required BOOLEAN,
    follow_up_date STRING,
    notes STRING,
    rec_add STRING,
    rec_edit STRING,
    processing_timestamp STRING,
    processing_date STRING
) PARTITIONED BY (glynac_organization_id, processing_date)
WITH (
    'connector' = 'filesystem',
    'path' = '{{MINIO_S3_ENDPOINT}}/redtail/activity/',
    'format' = 'orc',
    'sink.rolling-policy.rollover-interval' = '1day',
    'sink.rolling-policy.check-interval' = '1h'
);

-- UNNEST and write
INSERT INTO redtail_activity_storage
SELECT 
    COALESCE(redtail_activity_raw_input.scan_id, 'UNKNOWN') as scan_id,
    COALESCE(redtail_activity_raw_input.glynac_organization_id, 'unknown') as glynac_organization_id,
    COALESCE(redtail_activity_raw_input.entity_type, 'activity') as entity_type,
    COALESCE(redtail_activity_raw_input.`timestamp`, CAST(CURRENT_TIMESTAMP AS STRING)) as batch_timestamp,
    COALESCE(redtail_activity_raw_input.batch_number, 0) as batch_number,
    COALESCE(redtail_activity_raw_input.batch_offset, 0) as batch_offset,
    COALESCE(redtail_activity_raw_input.message_type, 'batch_data') as message_type,
    COALESCE(redtail_activity_raw_input.status, 'unknown') as batch_status,
    COALESCE(activity.id, 0) as activity_id,
    COALESCE(activity.rec_id, 0) as rec_id,
    COALESCE(activity.activity_type, 'Unknown') as activity_type,
    COALESCE(activity.category, '') as category,
    COALESCE(activity.subcategory, '') as subcategory,
    COALESCE(activity.subject, '') as subject,
    COALESCE(activity.description, '') as description,
    COALESCE(activity.contact_id, 0) as contact_id,
    COALESCE(activity.owner_id, 0) as owner_id,
    COALESCE(activity.status, 'Unknown') as activity_status,
    COALESCE(activity.priority, 'Normal') as priority,
    COALESCE(activity.activity_date, '') as activity_date,
    COALESCE(activity.duration_minutes, 0) as duration_minutes,
    COALESCE(activity.outcome, '') as outcome,
    COALESCE(activity.location, '') as location,
    COALESCE(activity.tags, '') as tags,
    COALESCE(activity.follow_up_required, false) as follow_up_required,
    COALESCE(activity.follow_up_date, '') as follow_up_date,
    COALESCE(activity.notes, '') as notes,
    COALESCE(activity.rec_add, '') as rec_add,
    COALESCE(activity.rec_edit, '') as rec_edit,
    CAST(redtail_activity_raw_input.processing_time AS STRING) as processing_timestamp,
    DATE_FORMAT(redtail_activity_raw_input.processing_time, 'yyyy-MM-dd') as processing_date
FROM redtail_activity_raw_input
CROSS JOIN UNNEST(records) AS activity;
