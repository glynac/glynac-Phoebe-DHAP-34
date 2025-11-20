-- ============================================================
-- GOLD: User Daily Metrics
-- ============================================================
-- Description: Daily aggregated user metrics for dashboards
-- Sources: silver.salesforce_user
-- Author: analytics-team
-- Type: SQL Aggregation
-- ============================================================

SELECT 
    processing_date as metric_date,
    glynac_organization_id as organization_id,
    
    -- User Counts
    COUNT(DISTINCT user_id) as total_users,
    COUNT(DISTINCT CASE WHEN is_active = 1 THEN user_id END) as active_users,
    COUNT(DISTINCT CASE WHEN is_active = 0 THEN user_id END) as inactive_users,
    
    -- New Users (created today)
    COUNT(DISTINCT CASE 
        WHEN toDate(created_date) = processing_date 
        THEN user_id 
    END) as new_users,
    
    -- Status Distribution
    COUNT(DISTINCT CASE WHEN status = 'active' THEN user_id END) as status_active,
    COUNT(DISTINCT CASE WHEN status = 'inactive' THEN user_id END) as status_inactive,
    
    -- Email Domains (Top 5)
    topK(5)(email_domain) as top_email_domains,
    
    -- User Age Statistics
    AVG(user_age_days) as avg_user_age_days,
    quantile(0.5)(user_age_days) as median_user_age_days,
    MIN(user_age_days) as min_user_age_days,
    MAX(user_age_days) as max_user_age_days,
    
    -- Cohort Analysis
    COUNT(DISTINCT CASE 
        WHEN user_age_days <= 30 
        THEN user_id 
    END) as users_0_30_days,
    
    COUNT(DISTINCT CASE 
        WHEN user_age_days > 30 AND user_age_days <= 90 
        THEN user_id 
    END) as users_31_90_days,
    
    COUNT(DISTINCT CASE 
        WHEN user_age_days > 90 AND user_age_days <= 365 
        THEN user_id 
    END) as users_91_365_days,
    
    COUNT(DISTINCT CASE 
        WHEN user_age_days > 365 
        THEN user_id 
    END) as users_over_1_year,
    
    -- Metadata
    now() as _calculated_at,
    'silver.salesforce_user' as _source_table

FROM {{ source('silver', 'salesforce_user') }}
WHERE processing_date = toDate('{{ ds }}')
GROUP BY processing_date, glynac_organization_id