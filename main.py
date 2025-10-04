import psycopg2
from config import get_engine, get_connection

conn = get_connection()


def run_query(cur, sql, description):
    print(f"\n--- {description} ---")
    cur.execute(sql)
    rows = cur.fetchall()
    for row in rows[:10]:  # limit to 10 rows in terminal output
        print(row)


def main():
    cur = conn.cursor()

    # A1. Average speed per road_type
    run_query(
        cur,
        """
        SELECT r.road_type, AVG(hs.avg_speed_kph) AS avg_speed
        FROM gauteng.historical_speeds hs
        JOIN gauteng.roads r ON hs.segment_id = r.segment_id
        GROUP BY r.road_type
        ORDER BY avg_speed DESC;
        """,
        "A1. Average speed per road type",
    )

    # A2. Top 5 municipalities with most incidents
    run_query(
        cur,
        """
        SELECT r.municipality, COUNT(i.incident_id) AS total_incidents
        FROM gauteng.incidents i
        JOIN gauteng.roads r ON i.affected_segment_id = r.segment_id
        GROUP BY r.municipality
        ORDER BY total_incidents DESC
        LIMIT 5;
        """,
        "A2. Top 5 municipalities with most incidents",
    )

    # A3. Average delivery distance & duration by priority
    run_query(
        cur,
        """
        SELECT priority,
               AVG(planned_distance_km) AS avg_distance,
               AVG(planned_duration_min) AS avg_duration
        FROM gauteng.assignments a
        JOIN gauteng.deliveries d ON a.delivery_id = d.delivery_id
        GROUP BY priority;
        """,
        "A3. Average delivery distance & duration by priority",
    )

    # A4. Daily average traffic speed
    run_query(
        cur,
        """
        SELECT DATE("timestamp") AS day, AVG(avg_speed_kph) AS avg_daily_speed
        FROM gauteng.historical_speeds
        GROUP BY day
        ORDER BY day;
        """,
        "A4. Daily average traffic speed",
    )

    # A5. Weather impact on average speeds
    run_query(
        cur,
        """
        SELECT CASE WHEN w.precip_mm_h > 0 THEN 'Rainy' ELSE 'Dry' END AS condition,
               AVG(hs.avg_speed_kph) AS avg_speed
        FROM gauteng.historical_speeds hs
        JOIN gauteng.weather w 
          ON hs.segment_id = w.nearest_segment_id
         AND DATE_TRUNC('hour', hs."timestamp") = DATE_TRUNC('hour', w."timestamp")
        GROUP BY condition;
        """,
        "A5. Weather impact on traffic speeds (Rainy vs Dry)",
    )

    # A6. Count hazmat truck deliveries
    run_query(
        cur,
        """
        SELECT COUNT(DISTINCT d.delivery_id) AS hazmat_deliveries
        FROM gauteng.deliveries d
        JOIN gauteng.truck_profiles t ON d.truck_id = t.truck_id
        WHERE t.hazmat = 1;
        """,
        "A6. Count of hazmat deliveries",
    )

    # A7. Top 10 busiest road segments
    run_query(
        cur,
        """
        SELECT segment_id, SUM(vehicle_count) AS total_vehicles
        FROM gauteng.historical_speeds
        GROUP BY segment_id
        ORDER BY total_vehicles DESC
        LIMIT 10;
        """,
        "A7. Top 10 busiest road segments",
    )

    # A8. Average cost per km vs per hour
    run_query(
        cur,
        """
        SELECT AVG(per_km_cost_rand) AS avg_cost_per_km,
               AVG(per_hour_cost_rand) AS avg_cost_per_hour
        FROM gauteng.deliveries;
        """,
        "A8. Average cost per km vs per hour",
    )

    # A9. Delivery punctuality
    run_query(
        cur,
        """
        SELECT d.delivery_id,
               (a.planned_arrival_utc - d.scheduled_arrival_utc) AS arrival_diff
        FROM gauteng.assignments a
        JOIN gauteng.deliveries d ON a.delivery_id = d.delivery_id
        ORDER BY arrival_diff DESC NULLS LAST
        LIMIT 10;
        """,
        "A9. Delivery punctuality (planned vs scheduled)",
    )

    # A10. Severity distribution of incidents
    run_query(
        cur,
        """
        SELECT severity, COUNT(*) AS count
        FROM gauteng.incidents
        GROUP BY severity
        ORDER BY severity;
        """,
        "A10. Severity distribution of incidents",
    )

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
