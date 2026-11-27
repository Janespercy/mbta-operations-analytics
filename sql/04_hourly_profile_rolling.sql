-- Average stop events per hour across the week, with a 3-hour rolling average
WITH hourly AS (
  SELECT
    t.hour,
    COUNT(*) / COUNT(DISTINCT f.date_key) AS avg_events_per_day,
    AVG(f.travel_time_seconds) AS avg_travel_s
  FROM fact_stop_event f
  JOIN dim_time t ON f.time_key = t.time_key
  GROUP BY t.hour
)
SELECT
  hour,
  ROUND(avg_events_per_day, 0) AS avg_events_per_day,
  ROUND(AVG(avg_events_per_day) OVER (ORDER BY hour ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING), 0) AS rolling_3h_avg,
  ROUND(avg_travel_s, 1) AS avg_travel_s
FROM hourly
ORDER BY hour
