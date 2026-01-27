-- ============================================================
-- Canonical Event Timeline - Base Table DDL
-- ============================================================
CREATE TABLE IF NOT EXISTS silver.org_123_timeline
(
    -- Core event fields
    event_id UUID,
    org_id Int32,
    event_type LowCardinality(String),
    timestamp DateTime64(3),
    entity_type LowCardinality(String),
    entity_id String,
    description String,

    -- Source tracking
    source_system LowCardinality(String),
    source_table String,
    source_id String,
    minio_path Nullable(String),

    -- Flexible metadata
    metadata Nullable(String),

    -- Partitioning
    processing_date Date,

    -- System fields
    _loaded_at DateTime DEFAULT now(),
    _version Int32 DEFAULT 1
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY (org_id, toYYYYMM(processing_date))
ORDER BY (org_id, entity_type, entity_id, timestamp, event_type)
PRIMARY KEY (org_id, entity_type, entity_id)
SETTINGS index_granularity = 8192;

-- Add indexes
ALTER TABLE silver.org_123_timeline
ADD INDEX IF NOT EXISTS event_type_idx event_type TYPE set GRANULARITY 4;

ALTER TABLE silver.org_123_timeline
ADD INDEX IF NOT EXISTS source_system_idx source_system TYPE set GRANULARITY 4;

ALTER TABLE silver.org_123_timeline
ADD INDEX IF NOT EXISTS timestamp_idx timestamp TYPE minmax GRANULARITY 1;

-- Add TTL (7 years retention)
ALTER TABLE silver.org_123_timeline
MODIFY TTL processing_date + INTERVAL 2555 DAY;
