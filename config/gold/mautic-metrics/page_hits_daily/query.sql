-- ============================================================
-- GOLD: Mautic Page Hits Daily
-- ============================================================
-- Description: Daily page hit metrics by country
-- Sources: mautic.page_hits_raw (via Silver layer)
-- Jinja Macros: ds, if_null()
-- ============================================================

SELECT
    toDate(date_hit) AS metric_date,
    {{ if_null('country', "'Unknown'", is_string=True) }} AS country,

    -- Hit Metrics
    count() AS total_hits,
    uniqExact(lead_id) AS unique_leads,
    uniqExact(url) AS unique_urls,

    -- Engagement Metrics
    avg(
        CASE
            WHEN date_left IS NOT NULL AND date_hit IS NOT NULL
            THEN dateDiff('second', date_hit, date_left)
            ELSE NULL
        END
    ) AS avg_time_on_page_seconds,

    -- Referrer Analysis
    countIf(referer IS NOT NULL AND referer != '') AS hits_with_referer,
    round(countIf(referer IS NOT NULL AND referer != '') * 100.0 / count(), 2) AS referer_rate,

    -- Metadata
    now() AS _aggregated_at,
    '{{ ds }}' AS _execution_date

FROM mautic.page_hits_raw

WHERE date_hit IS NOT NULL
  AND toDate(date_hit) = toDate('{{ ds }}')

GROUP BY metric_date, country
