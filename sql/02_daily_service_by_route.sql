-- Trips and stop events per route per day
SELECT
  d.date,
  d.day_name,
  r.route_name,
  COUNT(DISTINCT f.trip_id) AS trips,
  COUNT(*) AS stop_events
FROM fact_stop_event f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_route r ON f.route_id = r.route_id
GROUP BY d.date, d.day_name, r.route_name, r.route_sort_order
ORDER BY d.date, r.route_sort_order
