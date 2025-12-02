-- ============================================================
-- GOLD: User Daily Metrics
-- Aggregated daily user activity metrics
-- ============================================================
SELECT
    -- DIMENSIONS 
    toDate(processing_date) as metric_date,
    glynac_organization_id as organization_id,
    -- USER METRICS
    COUNT(DISTINCT user_id) as total_users,
    countIf(is_active = 1) as active_users,
    countIf(is_active = 0) as inactive_users,
    -- ENGAGEMENT METRICS
    AVG(user_age_days) as avg_user_age_days,
    MAX(user_age_days) as max_user_age_days,
    MIN(user_age_days) as min_user_age_days,
    -- ACTIVITY PERCENTILES
    quantile(0.5)(user_age_days) as median_user_age_days,
    quantile(0.95)(user_age_days) as p95_user_age_days,
    -- AGGREGATION METADATA
    now() as _aggregated_at,
    '{{ ds }}' as _execution_date
FROM silver.salesforce_user 
WHERE user_id IS NOT NULL 
GROUP BY 
    metric_date,
    organization_id
ORDER BY 
    metric_date DESC,
    organization_id