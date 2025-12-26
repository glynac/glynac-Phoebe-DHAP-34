SELECT
    toDate(date_added) AS metric_date,
    count() AS new_leads,
    countIf(date_identified IS NOT NULL) AS identified_leads,
    sum(COALESCE(points, 0)) AS total_points,
    avg(COALESCE(points, 0)) AS avg_points,
    countIf(email IS NOT NULL AND email != '') AS leads_with_email,
    countIf(company IS NOT NULL AND company != '') AS leads_with_company,
    now() AS _aggregated_at
FROM mautic.leads_raw
WHERE date_added IS NOT NULL
GROUP BY metric_date
