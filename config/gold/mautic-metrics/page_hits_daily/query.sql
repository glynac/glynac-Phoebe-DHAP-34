SELECT
    toDate(date_hit) AS metric_date,
    COALESCE(country, 'Unknown') AS country,
    count() AS total_hits,
    uniqExact(lead_id) AS unique_leads,
    uniqExact(url) AS unique_urls,
    avg(
        CASE
            WHEN date_left IS NOT NULL AND date_hit IS NOT NULL
            THEN dateDiff('second', date_hit, date_left)
            ELSE NULL
        END
    ) AS avg_time_on_page_seconds,
    countIf(referer IS NOT NULL AND referer != '') AS hits_with_referer,
    now() AS _aggregated_at
FROM mautic.page_hits_raw
WHERE date_hit IS NOT NULL
GROUP BY metric_date, country
