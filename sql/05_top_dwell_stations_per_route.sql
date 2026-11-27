-- Top 3 stations per route by median dwell time (stations with at least 50 dwell readings)
WITH station_dwell AS (
  SELECT
    f.route_id,
    s.station_name,
    COUNT(*) AS samples,
    percentile_approx(f.dwell_time_seconds, 0.5) AS median_dwell_s
  FROM fact_stop_event f
  JOIN dim_stop s ON f.stop_id = s.stop_id
  WHERE f.dwell_time_seconds IS NOT NULL
  GROUP BY f.route_id, s.station_name
  HAVING COUNT(*) >= 50
),
ranked AS (
  SELECT *, DENSE_RANK() OVER (PARTITION BY route_id ORDER BY median_dwell_s DESC) AS dwell_rank
  FROM station_dwell
)
SELECT r.route_name, k.station_name, k.median_dwell_s, k.samples, k.dwell_rank
FROM ranked k
JOIN dim_route r ON k.route_id = r.route_id
WHERE k.dwell_rank <= 3
ORDER BY r.route_sort_order, k.dwell_rank
