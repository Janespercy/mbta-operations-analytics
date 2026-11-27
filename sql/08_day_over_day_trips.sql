-- Trips per route per day and the change from the previous day
WITH daily AS (
  SELECT route_id, date_key, COUNT(DISTINCT trip_id) AS trips
  FROM fact_stop_event
  GROUP BY route_id, date_key
)
SELECT
  r.route_name,
  d.date,
  d.day_name,
  daily.trips,
  daily.trips - LAG(daily.trips) OVER (PARTITION BY daily.route_id ORDER BY daily.date_key) AS change_vs_prev_day
FROM daily
JOIN dim_route r ON daily.route_id = r.route_id
JOIN dim_date d ON daily.date_key = d.date_key
ORDER BY r.route_sort_order, d.date
