-- ============================================================
-- GOLD: Mautic Leads Daily Stats
-- ============================================================
-- Description: Daily aggregated lead metrics from Mautic
-- Sources: mautic.leads_raw (via Silver layer)
-- Jinja Macros: ds
-- ============================================================

SELECT
    toDate(date_added) AS metric_date,

    -- Lead Counts
    count() AS new_leads,
    countIf(date_identified IS NOT NULL) AS identified_leads,

    -- Points Metrics
    sum(COALESCE(points, 0)) AS total_points,
    avg(COALESCE(points, 0)) AS avg_points,
    max(COALESCE(points, 0)) AS max_points,

    -- Data Quality Metrics
    countIf(email IS NOT NULL AND email != '') AS leads_with_email,
    countIf(company IS NOT NULL AND company != '') AS leads_with_company,
    countIf(phone IS NOT NULL AND phone != '') AS leads_with_phone,

    -- Conversion Metrics
    round(countIf(date_identified IS NOT NULL) * 100.0 / count(), 2) AS identification_rate,

    -- Metadata
    now() AS _aggregated_at,
    '{{ ds }}' AS _execution_date

FROM mautic.leads_raw

WHERE date_added IS NOT NULL
  AND toDate(date_added) = toDate('{{ ds }}')

GROUP BY metric_date
