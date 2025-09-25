import psycopg2


def run_query(cur, sql, description):
    print(f"\n--- {description} ---")
    cur.execute(sql)
    rows = cur.fetchall()
    for row in rows[:10]:
        print(row)


def main():
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname="gauteng_db",
        user="postgres",
        password="1234",
        host="localhost",
        port="5432",
    )
    cur = conn.cursor()

    run_query(
        cur, "SELECT * FROM gauteng.roads LIMIT 10;", "Sample rows from roads table"
    )
    run_query(
        cur,
        """
        SELECT road_type, COUNT(*) 
        FROM gauteng.roads 
        GROUP BY road_type;
    """,
        "Number of roads per type",
    )
    run_query(
        cur,
        """
        SELECT municipality, AVG(speed_limit_kph) 
        FROM gauteng.roads 
        GROUP BY municipality 
        ORDER BY AVG(speed_limit_kph) DESC;
    """,
        "Average speed limit per municipality",
    )
    run_query(
        cur,
        """
        SELECT h.segment_id, AVG(h.avg_speed_kph) 
        FROM gauteng.historical_speeds h
        JOIN gauteng.roads r ON h.segment_id = r.segment_id
        GROUP BY h.segment_id
        ORDER BY AVG(h.avg_speed_kph) DESC;
    """,
        "Average speed per road segment (JOIN example)",
    )

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
