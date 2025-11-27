-- Redtail Notes Data Processing
-- Source: Read from Kafka with ARRAY<ROW> schema

CREATE TABLE redtail_note_raw_input (
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
        title STRING,
        content STRING,
        note_type STRING,
        contact_id INT,
        created_by INT,
        category STRING,
        tags STRING,
        is_private BOOLEAN,
        is_pinned BOOLEAN,
        rec_add STRING,
        rec_edit STRING,
        id INT
    )>,
    processing_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'redtail-notes-stream',
    'properties.bootstrap.servers' = '{{KAFKA_BOOTSTRAP_SERVERS}}',
    'properties.group.id' = 'redtail-note-orc-processor',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- Sink: Write to MinIO as ORC
CREATE TABLE redtail_note_storage (
    scan_id STRING,
    glynac_organization_id STRING,
    entity_type STRING,
    batch_timestamp STRING,
    batch_number INT,
    batch_offset BIGINT,
    status STRING,
    message_type STRING,
    note_rec_id INT,
    note_id INT,
    title STRING,
    content STRING,
    note_type STRING,
    contact_id INT,
    created_by INT,
    category STRING,
    tags STRING,
    is_private BOOLEAN,
    is_pinned BOOLEAN,
    rec_add STRING,
    rec_edit STRING,
    processing_timestamp STRING,
    processing_date STRING
) PARTITIONED BY (glynac_organization_id, processing_date)
WITH (
    'connector' = 'filesystem',
    'path' = 's3a://main-data/redtail/note/',
    'format' = 'orc',
    'sink.rolling-policy.rollover-interval' = '1day',
    'sink.rolling-policy.check-interval' = '1h'
);

-- UNNEST and write
INSERT INTO redtail_note_storage
SELECT 
    COALESCE(scan_id, 'UNKNOWN') as scan_id,
    COALESCE(glynac_organization_id, 'unknown') as glynac_organization_id,
    COALESCE(entity_type, 'note') as entity_type,
    COALESCE(`timestamp`, CAST(CURRENT_TIMESTAMP AS STRING)) as batch_timestamp,
    COALESCE(batch_number, 0) as batch_number,
    COALESCE(batch_offset, 0) as batch_offset,
    COALESCE(status, 'unknown') as status,
    COALESCE(message_type, 'batch_data') as message_type,
    COALESCE(note.rec_id, 0) as note_rec_id,
    COALESCE(note.id, 0) as note_id,
    COALESCE(note.title, '') as title,
    COALESCE(note.content, '') as content,
    COALESCE(note.note_type, '') as note_type,
    COALESCE(note.contact_id, 0) as contact_id,
    COALESCE(note.created_by, 0) as created_by,
    COALESCE(note.category, '') as category,
    COALESCE(note.tags, '') as tags,
    COALESCE(note.is_private, false) as is_private,
    COALESCE(note.is_pinned, false) as is_pinned,
    COALESCE(note.rec_add, '') as rec_add,
    COALESCE(note.rec_edit, '') as rec_edit,
    CAST(processing_time AS STRING) as processing_timestamp,
    DATE_FORMAT(processing_time, 'yyyy-MM-dd') as processing_date
FROM redtail_note_raw_input
CROSS JOIN UNNEST(records) AS note;
