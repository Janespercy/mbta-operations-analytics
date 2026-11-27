-- Average segment travel time and dwell time, weekday vs weekend, by route
SELECT
  r.route_name,
  ROUND(AVG(CASE WHEN NOT d.is_weekend THEN f.travel_time_seconds END), 1) AS weekday_travel_s,
  ROUND(AVG(CASE WHEN d.is_weekend THEN f.travel_time_seconds END), 1) AS weekend_travel_s,
  ROUND(AVG(CASE WHEN NOT d.is_weekend THEN f.dwell_time_seconds END), 1) AS weekday_dwell_s,
  ROUND(AVG(CASE WHEN d.is_weekend THEN f.dwell_time_seconds END), 1) AS weekend_dwell_s
FROM fact_stop_event f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_route r ON f.route_id = r.route_id
GROUP BY r.route_name, r.route_sort_order
ORDER BY r.route_sort_order
