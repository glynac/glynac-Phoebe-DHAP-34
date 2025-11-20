
-- Create source table (Kafka)
CREATE TABLE example_source (
    id INT,
    name STRING,
    value DOUBLE,
    processing_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'example-topic',
    'properties.bootstrap.servers' = '{{KAFKA_BOOTSTRAP_SERVERS}}',
    'properties.group.id' = 'example-processor',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- Create sink table (MinIO)
CREATE TABLE example_sink (
    id INT,
    name STRING,
    value DOUBLE,
    processing_timestamp STRING,
    processing_date STRING
) PARTITIONED BY (processing_date)
WITH (
    'connector' = 'filesystem',
    'path' = '{{MINIO_S3_ENDPOINT}}/example/data/',
    'format' = 'orc',
    'sink.rolling-policy.rollover-interval' = '1day',
    'sink.rolling-policy.check-interval' = '1h'
);

-- Insert statement
INSERT INTO example_sink
SELECT
    id,
    name,
    value,
    CAST(processing_time AS STRING) as processing_timestamp,
    DATE_FORMAT(processing_time, 'yyyy-MM-dd') as processing_date
FROM example_source;
