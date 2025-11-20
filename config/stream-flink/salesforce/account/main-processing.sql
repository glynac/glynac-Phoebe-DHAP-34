-- Salesforce Account Data Processing
-- Source: Read from Kafka with ARRAY<ROW> schema

CREATE TABLE salesforce_account_raw_input (
    scan_id STRING,
    service STRING,
    `timestamp` STRING,
    `offset` BIGINT,
    page INT,
    page_size INT,
    batch_size INT,
    total_streamed INT,
    total_data_streamed INT,
    total_available INT,
    table_name STRING,
    columns ARRAY<STRING>,
    available_tables ARRAY<STRING>,
    glynac_organization_id STRING,
    data ARRAY<ROW(
        attributes__type STRING,
        attributes__url STRING,
        id STRING,
        name STRING,
        type STRING,
        industry STRING,
        billing_street STRING,
        billing_city STRING,
        billing_state STRING,
        billing_country STRING,
        phone STRING,
        website STRING,
        annual_revenue DOUBLE,
        number_of_employees INT,
        owner_id STRING,
        created_date STRING,
        last_modified_date STRING,
        _extracted_at STRING,
        _scan_id STRING,
        _organization_id STRING,
        _page_number INT,
        _source_service STRING,
        _token_refresh_supported BOOLEAN,
        _dlt_load_id STRING,
        _dlt_id STRING,
        billing_postal_code STRING
    )>,
    processing_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'sf-stream-accounts',
    'properties.bootstrap.servers' = '{{KAFKA_BOOTSTRAP_SERVERS}}',
    'properties.group.id' = 'salesforce-account-orc-processor',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- Sink: Write to MinIO as ORC
CREATE TABLE salesforce_account_storage (
    scan_id STRING,
    glynac_organization_id STRING,
    service STRING,
    batch_timestamp STRING,
    page_number INT,
    table_name STRING,
    account_id STRING,
    account_name STRING,
    account_type STRING,
    industry STRING,
    billing_street STRING,
    billing_city STRING,
    billing_state STRING,
    billing_country STRING,
    billing_postal_code STRING,
    phone STRING,
    website STRING,
    annual_revenue DOUBLE,
    number_of_employees INT,
    owner_id STRING,
    created_date STRING,
    last_modified_date STRING,
    extracted_at STRING,
    source_service STRING,
    processing_timestamp STRING,
    processing_date STRING
) PARTITIONED BY (glynac_organization_id, processing_date)
WITH (
    'connector' = 'filesystem',
    'path' = '{{MINIO_S3_ENDPOINT}}/salesforce/account/',
    'format' = 'orc',
    'sink.rolling-policy.rollover-interval' = '1day',
    'sink.rolling-policy.check-interval' = '1h'
);

-- UNNEST and write
INSERT INTO salesforce_account_storage
SELECT 
    COALESCE(scan_id, 'UNKNOWN') as scan_id,
    COALESCE(glynac_organization_id, 'unknown') as glynac_organization_id,
    COALESCE(service, 'salesforce_account') as service,
    COALESCE(`timestamp`, CAST(CURRENT_TIMESTAMP AS STRING)) as batch_timestamp,
    COALESCE(page, 0) as page_number,
    COALESCE(table_name, 'salesforce_accounts') as table_name,
    COALESCE(account.id, 'NONE') as account_id,
    COALESCE(account.name, 'No Name') as account_name,
    COALESCE(account.type, 'Unknown') as account_type,
    COALESCE(account.industry, 'Unknown') as industry,
    COALESCE(account.billing_street, '') as billing_street,
    COALESCE(account.billing_city, '') as billing_city,
    COALESCE(account.billing_state, '') as billing_state,
    COALESCE(account.billing_country, '') as billing_country,
    COALESCE(account.billing_postal_code, '') as billing_postal_code,
    COALESCE(account.phone, '') as phone,
    COALESCE(account.website, '') as website,
    COALESCE(account.annual_revenue, 0.0) as annual_revenue,
    COALESCE(account.number_of_employees, 0) as number_of_employees,
    COALESCE(account.owner_id, '') as owner_id,
    COALESCE(account.created_date, '') as created_date,
    COALESCE(account.last_modified_date, '') as last_modified_date,
    COALESCE(account._extracted_at, '') as extracted_at,
    COALESCE(account._source_service, 'salesforce') as source_service,
    CAST(processing_time AS STRING) as processing_timestamp,
    DATE_FORMAT(processing_time, 'yyyy-MM-dd') as processing_date
FROM salesforce_account_raw_input
CROSS JOIN UNNEST(data) AS account
WHERE scan_id IS NOT NULL;