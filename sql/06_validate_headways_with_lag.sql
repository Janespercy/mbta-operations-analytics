-- Recompute trunk headways from arrival times with LAG and compare to LAMP's headway_trunk_seconds.
-- Low agreement would mean LAMP measures headway differently (e.g. on departures), which needs documenting.
WITH ordered AS (
  SELECT
    f.headway_trunk_seconds,
    unix_timestamp(f.stop_ts) - unix_timestamp(LAG(f.stop_ts) OVER (
      PARTITION BY f.date_key, f.stop_id, f.trunk_route_id, f.direction_id
      ORDER BY f.stop_ts
    )) AS my_headway
  FROM fact_stop_event f
  WHERE f.stop_ts IS NOT NULL
)
SELECT
  COUNT(*) AS compared,
  ROUND(100 * AVG(CASE WHEN ABS(my_headway - headway_trunk_seconds) <= 30 THEN 1 ELSE 0 END), 1) AS pct_within_30s,
  ROUND(100 * AVG(CASE WHEN ABS(my_headway - headway_trunk_seconds) <= 120 THEN 1 ELSE 0 END), 1) AS pct_within_2min,
  percentile_approx(ABS(my_headway - headway_trunk_seconds), 0.5) AS median_abs_diff_s
FROM ordered
WHERE my_headway IS NOT NULL AND headway_trunk_seconds IS NOT NULL
