-- ============================================================
-- GOLD: Customer Lifetime Value (CLV) - SQL Preprocessing
-- Prepare customer data for CLV calculation
-- ============================================================

SELECT
    customer_id,
    glynac_organization_id as organization_id,
    
    -- Customer attributes
    MIN(first_purchase_date) as first_purchase_date,
    MAX(last_purchase_date) as last_purchase_date,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(order_value) as total_revenue,
    AVG(order_value) as avg_order_value,
    
    -- Recency, Frequency, Monetary
    dateDiff('day', MAX(last_purchase_date), today()) as recency_days,
    COUNT(DISTINCT order_id) as frequency,
    SUM(order_value) as monetary,
    
    -- Time-based features
    dateDiff('day', MIN(first_purchase_date), MAX(last_purchase_date)) as customer_lifespan_days,
    
    -- Aggregation metadata
    toDate('{{ ds }}') as calculation_date

FROM {{ source('silver', 'customer_orders') }}

WHERE processing_date <= toDate('{{ ds }}')

GROUP BY 
    customer_id,
    organization_id

HAVING 
    total_orders >= 1  -- At least 1 purchase