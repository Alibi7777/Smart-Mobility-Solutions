import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from app_config import get_engine, get_connection

engine = get_engine()
conn = get_connection()

# JOINs to bring together time, roads, and speed data
query = """
SELECT
    hs.timestamp,
    r.municipality,
    AVG(hs.avg_speed_kph) AS avg_speed,
    AVG(hs.vehicle_count) AS avg_vehicles
FROM gauteng.historical_speeds hs
LEFT JOIN gauteng.roads r ON hs.segment_id = r.segment_id
LEFT JOIN gauteng.weather w
    ON hs.segment_id = w.nearest_segment_id
   AND DATE_TRUNC('hour', hs.timestamp) = DATE_TRUNC('hour', w.timestamp)
GROUP BY hs.timestamp, r.municipality
ORDER BY hs.timestamp;
"""

df = pd.read_sql(query, engine)

# Converting timestamp to date
df["time"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M")


fig = px.scatter(
    df,
    x="avg_vehicles",
    y="avg_speed",
    animation_frame="time",
    color="municipality",
    size="avg_vehicles",
    title="Traffic Speed vs Vehicle Volume Over Time in Gauteng",
    labels={
        "avg_speed": "Average Speed (km/h)",
        "avg_vehicles": "Average Vehicle Count (per 5 min)",
        "municipality": "Municipality",
    },
)

fig.update_layout(
    xaxis=dict(title="Average Vehicle Count (per 5 min)"),
    yaxis=dict(title="Average Speed (km/h)"),
    legend_title="Municipality",
)

fig.show()
