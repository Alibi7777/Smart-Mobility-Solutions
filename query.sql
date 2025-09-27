-- ===========================
-- Gauteng Traffic Analytics
-- queries.sql
-- ===========================
-- This file contains example SQL queries with comments.
-- Each query demonstrates required functionality for the assignment.
-- ===========================

-- 1. Sanity check: preview first 10 rows of roads
SELECT * FROM gauteng.roads LIMIT 10;

-- 2. Filtering + sorting: get all incidents with severity >= 3, sorted by timestamp (most recent first)
SELECT incident_id, type, severity, "timestamp", affected_segment_id
FROM gauteng.incidents
WHERE severity >= 3
ORDER BY "timestamp" DESC;

-- 3. Aggregation: count how many road segments exist per municipality
SELECT municipality, COUNT(*) AS num_segments
FROM gauteng.roads
GROUP BY municipality
ORDER BY num_segments DESC;

-- 4. Aggregation with MIN/MAX/AVG: average, min, and max vehicle counts by road_type
SELECT road_type,
       AVG(vehicle_count) AS avg_volume,
       MIN(vehicle_count) AS min_volume,
       MAX(vehicle_count) AS max_volume
FROM gauteng.historical_speeds hs
JOIN gauteng.roads r ON hs.segment_id = r.segment_id
GROUP BY road_type;

-- 5. JOIN example: find deliveries with truck details
SELECT d.delivery_id, d.origin_name, d.destination_name,
       t.max_weight_tons, t.hazmat
FROM gauteng.deliveries d
JOIN gauteng.truck_profiles t ON d.truck_id = t.truck_id
LIMIT 10;

-- ===========================
-- Analytical Topics (10 queries)
-- ===========================

-- A1. Average speed per road_type
SELECT r.road_type, AVG(hs.avg_speed_kph) AS avg_speed
FROM gauteng.historical_speeds hs
JOIN gauteng.roads r ON hs.segment_id = r.segment_id
GROUP BY r.road_type
ORDER BY avg_speed DESC;

-- A2. Top 5 municipalities with the most road incidents
SELECT r.municipality, COUNT(i.incident_id) AS total_incidents
FROM gauteng.incidents i
JOIN gauteng.roads r ON i.affected_segment_id = r.segment_id
GROUP BY r.municipality
ORDER BY total_incidents DESC
LIMIT 5;

-- A3. Average delivery distance and duration by priority
SELECT priority,
       AVG(planned_distance_km) AS avg_distance,
       AVG(planned_duration_min) AS avg_duration
FROM gauteng.assignments a
JOIN gauteng.deliveries d ON a.delivery_id = d.delivery_id
GROUP BY priority;

-- A4. Daily average traffic speed (time series)
SELECT DATE("timestamp") AS day, AVG(avg_speed_kph) AS avg_daily_speed
FROM gauteng.historical_speeds
GROUP BY day
ORDER BY day;

-- A5. Weather impact: compare average speeds on rainy vs non-rainy conditions
SELECT CASE WHEN w.precip_mm_h > 0 THEN 'Rainy' ELSE 'Dry' END AS condition,
       AVG(hs.avg_speed_kph) AS avg_speed
FROM gauteng.historical_speeds hs
JOIN gauteng.weather w ON hs.segment_id = w.nearest_segment_id
   AND DATE_TRUNC('hour', hs."timestamp") = DATE_TRUNC('hour', w."timestamp")
GROUP BY condition;

-- A6. Count of hazmat trucks used in deliveries
SELECT COUNT(DISTINCT d.delivery_id) AS hazmat_deliveries
FROM gauteng.deliveries d
JOIN gauteng.truck_profiles t ON d.truck_id = t.truck_id
WHERE t.hazmat = 1;

-- A7. Top 10 busiest road segments by vehicle_count
SELECT segment_id, SUM(vehicle_count) AS total_vehicles
FROM gauteng.historical_speeds
GROUP BY segment_id
ORDER BY total_vehicles DESC
LIMIT 10;

-- A8. Average cost per km vs per hour for deliveries
SELECT AVG(per_km_cost_rand) AS avg_cost_per_km,
       AVG(per_hour_cost_rand) AS avg_cost_per_hour
FROM gauteng.deliveries;

-- A9. Delivery punctuality: compare planned vs scheduled arrival times
SELECT d.delivery_id,
       (a.planned_arrival_utc - d.scheduled_arrival_utc) AS arrival_diff
FROM gauteng.assignments a
JOIN gauteng.deliveries d ON a.delivery_id = d.delivery_id
ORDER BY arrival_diff DESC NULLS LAST
LIMIT 10;

-- A10. Severity distribution of incidents
SELECT severity, COUNT(*) AS count
FROM gauteng.incidents
GROUP BY severity
ORDER BY severity;

-- ===========================
-- END OF FILE
-- ===========================
