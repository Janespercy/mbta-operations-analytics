-- Does excluding ADDED trips change headway adherence? If Green Line D stays lowest,
-- its poor adherence is real; if it improves a lot, ADDED trips were distorting the metric.
WITH scored AS (
  SELECT
    f.route_id,
    f.is_added_trip,
    CASE WHEN f.headway_trunk_seconds <= 1.5 * f.scheduled_headway_trunk THEN 1 ELSE 0 END AS within_target
  FROM fact_stop_event f
  WHERE f.headway_trunk_seconds IS NOT NULL
    AND f.scheduled_headway_trunk > 0
)
SELECT
  r.route_name,
  COUNT(*) AS headways,
  ROUND(100 * AVG(CASE WHEN s.is_added_trip THEN 1 ELSE 0 END), 1) AS pct_added,
  ROUND(100 * AVG(s.within_target), 1) AS pct_within_all,
  ROUND(100 * AVG(CASE WHEN NOT s.is_added_trip THEN s.within_target END), 1) AS pct_within_scheduled_only,
  ROUND(100 * AVG(CASE WHEN s.is_added_trip THEN s.within_target END), 1) AS pct_within_added_only
FROM scored s
JOIN dim_route r ON s.route_id = r.route_id
GROUP BY r.route_name, r.route_sort_order
ORDER BY pct_within_scheduled_only
