SELECT
    toMonday(toDate(l.date_added)) AS week_start,
    uniqState(l.id) AS total_leads_state,
    countState() AS total_hits_state,
    avgState(l.points) AS avg_points_state,
    maxState(l.points) AS max_points_state,
    now() AS _aggregated_at
FROM mautic.leads_raw l
WHERE l.date_added IS NOT NULL
GROUP BY week_start
