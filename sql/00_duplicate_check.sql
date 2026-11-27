-- Are the rejected duplicates exact copies of the kept row, or different readings of the same event?
SELECT
  COUNT(*) AS rejected_duplicates,
  SUM(CASE WHEN r.vehicle_id = f.vehicle_id THEN 1 ELSE 0 END) AS same_vehicle,
  SUM(CASE WHEN r.stop_id = f.stop_id THEN 1 ELSE 0 END) AS same_stop,
  SUM(CASE WHEN timestamp_seconds(r.stop_timestamp) = f.stop_ts THEN 1 ELSE 0 END) AS same_arrival_time,
  SUM(CASE WHEN r.stop_timestamp IS NULL AND f.stop_ts IS NULL THEN 1 ELSE 0 END) AS both_arrivals_null
FROM rejected_events r
JOIN fact_stop_event f
  ON f.date_key = CAST(r.service_date AS INT)
 AND f.trip_id = r.trip_id
 AND f.stop_sequence = r.stop_sequence
