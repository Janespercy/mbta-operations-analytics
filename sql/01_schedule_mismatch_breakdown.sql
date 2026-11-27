-- Where do the out-of-range delays come from? If it's mostly ADDED trips,
-- the problem is the schedule match, not the trains.
SELECT
  r.route_name,
  f.is_added_trip,
  COUNT(*) AS events,
  SUM(CASE WHEN f.is_schedule_mismatch THEN 1 ELSE 0 END) AS mismatches,
  ROUND(100 * AVG(CASE WHEN f.is_schedule_mismatch THEN 1 ELSE 0 END), 1) AS pct_mismatch
FROM fact_stop_event f
JOIN dim_route r ON f.route_id = r.route_id
GROUP BY r.route_name, r.route_sort_order, f.is_added_trip
ORDER BY r.route_sort_order, f.is_added_trip
