SELECT
    toDate(ph.date_hit) AS metric_date,
    COALESCE(ph.source, 'direct') AS source,
    uniqExact(ph.lead_id) AS unique_leads,
    count() AS total_visits,
    uniqExactIf(ph.lead_id, ph.date_hit = (
        SELECT min(ph2.date_hit)
        FROM mautic.page_hits_raw ph2
        WHERE ph2.lead_id = ph.lead_id
    )) AS first_touch_leads,
    now() AS _aggregated_at
FROM mautic.page_hits_raw ph
WHERE ph.date_hit IS NOT NULL
  AND ph.lead_id IS NOT NULL
GROUP BY metric_date, source
