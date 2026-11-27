-- Headway adherence by route and time of day.
-- Project definitions: a headway is "within target" if it's at most 1.5x the scheduled headway,
-- and "bunched" if it's under half the scheduled headway.
WITH scored AS (
  SELECT
    f.route_id,
    t.time_period,
    t.hour,
    f.headway_trunk_seconds AS actual,
    f.scheduled_headway_trunk AS scheduled
  FROM fact_stop_event f
  JOIN dim_time t ON f.time_key = t.time_key
  WHERE f.headway_trunk_seconds IS NOT NULL
    AND f.scheduled_headway_trunk > 0
)
SELECT
  r.route_name,
  s.time_period,
  COUNT(*) AS headways,
  ROUND(100 * AVG(CASE WHEN actual <= 1.5 * scheduled THEN 1 ELSE 0 END), 1) AS pct_within_target,
  ROUND(100 * AVG(CASE WHEN actual < 0.5 * scheduled THEN 1 ELSE 0 END), 1) AS pct_bunched,
  ROUND(AVG(actual) / 60, 1) AS avg_actual_min,
  ROUND(AVG(scheduled) / 60, 1) AS avg_scheduled_min
FROM scored s
JOIN dim_route r ON s.route_id = r.route_id
GROUP BY r.route_name, r.route_sort_order, s.time_period
ORDER BY r.route_sort_order, MIN(s.hour)
