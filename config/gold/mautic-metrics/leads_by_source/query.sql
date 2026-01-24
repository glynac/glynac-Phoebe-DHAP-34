-- ============================================================
-- GOLD: Mautic Leads By Source
-- ============================================================
-- Description: Daily lead attribution by traffic source
-- Sources: mautic.page_hits_raw (via Silver layer)
-- Jinja Macros: ds, if_null()
-- ============================================================

SELECT
    toDate(ph.date_hit) AS metric_date,
    {{ if_null('ph.source', "'direct'", is_string=True) }} AS traffic_source,

    -- Lead Metrics
    uniqExact(ph.lead_id) AS unique_leads,
    count() AS total_visits,

    -- First Touch Attribution
    uniqExactIf(ph.lead_id, ph.date_hit = (
        SELECT min(ph2.date_hit)
        FROM mautic.page_hits_raw ph2
        WHERE ph2.lead_id = ph.lead_id
          AND toDate(ph2.date_hit) <= toDate('{{ ds }}')
    )) AS first_touch_leads,

    -- Engagement
    round(count() * 1.0 / nullIf(uniqExact(ph.lead_id), 0), 2) AS visits_per_lead,

    -- Metadata
    now() AS _aggregated_at,
    '{{ ds }}' AS _execution_date

FROM mautic.page_hits_raw ph

WHERE ph.date_hit IS NOT NULL
  AND ph.lead_id IS NOT NULL
  AND toDate(ph.date_hit) = toDate('{{ ds }}')

GROUP BY metric_date, traffic_source
