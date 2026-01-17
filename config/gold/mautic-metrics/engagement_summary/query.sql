-- ============================================================
-- GOLD: Mautic Engagement Summary (Weekly)
-- ============================================================
-- Description: Weekly engagement aggregates using AggregatingMergeTree states
-- Sources: mautic.leads_raw (via Silver layer)
-- Jinja Macros: ds, aggregate_state()
-- Note: Uses State functions for AggregatingMergeTree engine
-- ============================================================

SELECT
    toMonday(toDate(l.date_added)) AS week_start,

    -- Aggregate States for AggregatingMergeTree
    {{ aggregate_state('uniq', 'l.id') }} AS total_leads_state,
    {{ aggregate_state('count', '*') }} AS total_records_state,
    {{ aggregate_state('avg', 'l.points') }} AS avg_points_state,
    {{ aggregate_state('max', 'l.points') }} AS max_points_state,
    {{ aggregate_state('min', 'l.points') }} AS min_points_state,
    {{ aggregate_state('sum', 'l.points') }} AS total_points_state,

    -- Metadata
    now() AS _aggregated_at,
    '{{ ds }}' AS _execution_date

FROM mautic.leads_raw l

WHERE l.date_added IS NOT NULL
  -- Process data for current week
  AND toMonday(toDate(l.date_added)) = toMonday(toDate('{{ ds }}'))

GROUP BY week_start
