-- Station-to-station segments running furthest over scheduled travel time.
-- LAG gives the previous recorded stop on the same trip.
WITH seg AS (
  SELECT
    f.route_id,
    f.stop_id,
    LAG(f.stop_id) OVER (PARTITION BY f.date_key, f.trip_id ORDER BY f.stop_sequence) AS prev_stop_id,
    f.travel_time_seconds,
    f.scheduled_travel_time
  FROM fact_stop_event f
)
SELECT
  r.route_name,
  ps.station_name AS from_station,
  s.station_name AS to_station,
  COUNT(*) AS runs,
  ROUND(AVG(seg.travel_time_seconds), 0) AS avg_actual_s,
  ROUND(AVG(seg.scheduled_travel_time), 0) AS avg_scheduled_s,
  ROUND(AVG(seg.travel_time_seconds - seg.scheduled_travel_time), 0) AS avg_excess_s
FROM seg
JOIN dim_stop s ON seg.stop_id = s.stop_id
JOIN dim_stop ps ON seg.prev_stop_id = ps.stop_id
JOIN dim_route r ON seg.route_id = r.route_id
WHERE seg.travel_time_seconds IS NOT NULL
  AND seg.scheduled_travel_time IS NOT NULL
GROUP BY r.route_name, ps.station_name, s.station_name
HAVING COUNT(*) >= 100
ORDER BY avg_excess_s DESC
LIMIT 15
