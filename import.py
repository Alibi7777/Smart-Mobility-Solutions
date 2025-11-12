# import.py
from __future__ import annotations

# --- force-load local app_config.py next to this file (avoids pip 'config' clash) ---
from pathlib import Path
from importlib.util import spec_from_file_location, module_from_spec

CFG_PATH = Path(__file__).with_name("app_config.py")
if not CFG_PATH.exists():
    raise SystemExit(f"app_config.py not found next to import.py: {CFG_PATH}")

spec = spec_from_file_location("app_config", str(CFG_PATH))
config = module_from_spec(spec)  # type: ignore[assignment]
assert spec.loader is not None
spec.loader.exec_module(config)  # type: ignore[arg-type]
print(f"[config] loaded from: {CFG_PATH}")
# -------------------------------------------------------------------------------

import os
import json
import tempfile
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection


# -------- helpers --------
def ensure_json(series: pd.Series) -> pd.Series:
    """Make sure 'route_segments' is a valid JSON string for COPY."""
    out: List[str] = []
    for v in series.fillna("[]"):
        if isinstance(v, (list, dict)):
            out.append(json.dumps(v))
        else:
            s = str(v).strip()
            if (s.startswith("[") and s.endswith("]")) or (
                s.startswith("{") and s.endswith("}")
            ):
                out.append(s)
            elif s == "":
                out.append("[]")
            else:
                out.append(json.dumps([x.strip() for x in s.split(",") if x.strip()]))
    return pd.Series(out)


def copy_with_temp(
    conn: Connection,
    csv_path: Path,
    target_table: str,
    cols: List[str],
    casts: Optional[Dict[str, str]] = None,
    key_cols: Optional[List[str]] = None,
    upsert: bool = False,
) -> None:
    """
    Fast & safe load:
      1) COPY csv → temp table (all TEXT)
      2) INSERT into final table with explicit CASTs
      3) optional ON CONFLICT DO NOTHING on key_cols
    """
    schema = config.DB_SCHEMA
    tmp = f"tmp_{target_table.replace('.', '_')}"
    fq_tmp = f"{schema}.{tmp}"
    fq_target = f"{schema}.{target_table}"

    # 1) temp table
    conn.execute(text(f"DROP TABLE IF EXISTS {fq_tmp}"))
    cols_sql = ", ".join([f"{c} TEXT" for c in cols])
    conn.execute(text(f"CREATE TABLE {fq_tmp} ({cols_sql})"))

    # 2) COPY
    with open(csv_path, "r", encoding="utf-8") as f:
        conn.connection.cursor().copy_expert(
            f"COPY {fq_tmp} ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',')",
            f,
        )

    # 3) INSERT with casts
    casts = casts or {}
    select_exprs: List[str] = []
    for c in cols:
        expr = casts.get(c)
        select_exprs.append(expr.format(col=c) if expr else c)
    select_sql = ", ".join(select_exprs)

    if upsert and key_cols:
        conflict = ", ".join(key_cols)
        sql = f"""
            INSERT INTO {fq_target} ({', '.join(cols)})
            SELECT {select_sql} FROM {fq_tmp}
            ON CONFLICT ({conflict}) DO NOTHING
        """
    else:
        sql = f"""
            INSERT INTO {fq_target} ({', '.join(cols)})
            SELECT {select_sql} FROM {fq_tmp}
        """
    conn.execute(text(sql))
    conn.execute(text(f"DROP TABLE IF EXISTS {fq_tmp}"))


def file_in(dirpath: Path, name: str) -> Optional[Path]:
    p = dirpath / name
    return p if p.exists() else None


# -------- per-table importers --------
def import_roads(conn: Connection, data_dir: Path) -> None:
    """
    Flexible loader for roads.csv:
    - tolerates extra columns (e.g., name) and different header orders
    - maps aliases and reorders to target schema columns
    """
    path = file_in(data_dir, "roads.csv")
    if not path:
        return

    # target order for COPY
    cols = [
        "segment_id",
        "road_id",
        "from_lat",
        "from_lon",
        "to_lat",
        "to_lon",
        "length_m",
        "road_type",
        "lanes",
        "speed_limit_kph",
        "oneway",
        "municipality",
        "province",
    ]

    # read & normalize
    df = pd.read_csv(path)

    # common header aliases seen in datasets
    alias = {
        "segment": "segment_id",
        "segmentID": "segment_id",
        "seg_id": "segment_id",
        "road": "road_id",
        "roadID": "road_id",
        "road_name": None,  # drop if present
        "name": None,  # drop if present
        "from_latitude": "from_lat",
        "from_longitude": "from_lon",
        "to_latitude": "to_lat",
        "to_longitude": "to_lon",
        "length_meters": "length_m",
        "length": "length_m",
        "speed_limit": "speed_limit_kph",
        "one_way": "oneway",
        "city": "municipality",
        "muni": "municipality",
        "state": "province",
        "prov": "province",
    }

    # rename known aliases, drop unknown extras later
    rename_map = {c: alias[c] for c in df.columns if c in alias and alias[c]}
    df = df.rename(columns=rename_map)

    # if both municipality/province present but swapped, it's fine; we select by name
    # drop columns we don't need
    keep = set(cols)
    df = df[[c for c in df.columns if c in keep]]

    # ensure ALL required columns exist (fill missing with empty)
    for c in cols:
        if c not in df.columns:
            df[c] = ""

    # reorder columns to exact target order
    df = df[cols]

    # write a temp CSV that matches exactly our COPY column list
    tmp = Path(tempfile.mkstemp(prefix="roads_", suffix=".csv")[1])
    df.to_csv(tmp, index=False)

    casts = {
        "from_lat": "CAST({col} AS NUMERIC(9,6))",
        "from_lon": "CAST({col} AS NUMERIC(9,6))",
        "to_lat": "CAST({col} AS NUMERIC(9,6))",
        "to_lon": "CAST({col} AS NUMERIC(9,6))",
        "length_m": "NULLIF({col}, '')::NUMERIC(10,1)",
        "lanes": "NULLIF({col}, '')::INT",
        "speed_limit_kph": "NULLIF({col}, '')::INT",
        "oneway": "NULLIF({col}, '')::INT",
        "road_type": f"{config.DB_SCHEMA}.road_enum({{col}})",
    }

    try:
        copy_with_temp(
            conn, tmp, "roads", cols, casts=casts, key_cols=["segment_id"], upsert=True
        )
    finally:
        try:
            os.remove(tmp)
        except:
            pass


def import_historical_speeds(conn: Connection, data_dir: Path) -> None:
    path = file_in(data_dir, "historical_speeds.csv")
    if not path:
        return
    cols = [
        "segment_id",
        "timestamp",
        "avg_speed_kph",
        "pct_freeflow",
        "vehicle_count",
        "interval_minutes",
        "source",
    ]
    casts = {
        "timestamp": "CAST({col} AS TIMESTAMPTZ)",
        "avg_speed_kph": "NULLIF({col}, '')::NUMERIC(6,1)",
        "pct_freeflow": "NULLIF({col}, '')::NUMERIC(5,3)",
        "vehicle_count": "NULLIF({col}, '')::INT",
        "interval_minutes": "NULLIF({col}, '')::INT",
    }
    copy_with_temp(
        conn,
        path,
        "historical_speeds",
        cols,
        casts=casts,
        key_cols=["segment_id", "timestamp"],
        upsert=True,
    )


def import_incidents(conn: Connection, data_dir: Path) -> None:
    path = file_in(data_dir, "incidents.csv")
    if not path:
        return
    cols = [
        "incident_id",
        "timestamp",
        "type",
        "severity",
        "lat",
        "lon",
        "affected_segment_id",
        "description",
        "source",
    ]
    casts = {
        "timestamp": "CAST({col} AS TIMESTAMPTZ)",
        "type": f"{config.DB_SCHEMA}.incident_enum({{col}})",
        "severity": "NULLIF({col}, '')::INT",
        "lat": "NULLIF({col}, '')::NUMERIC(9,6)",
        "lon": "NULLIF({col}, '')::NUMERIC(9,6)",
    }
    copy_with_temp(
        conn,
        path,
        "incidents",
        cols,
        casts=casts,
        key_cols=["incident_id"],
        upsert=True,
    )


def import_weather(conn: Connection, data_dir: Path) -> None:
    path = file_in(data_dir, "weather.csv")
    if not path:
        return
    cols = [
        "station_id",
        "timestamp",
        "lat",
        "lon",
        "temperature_c",
        "precip_mm_h",
        "wind_kph",
        "visibility_km",
        "wx_condition",
        "nearest_segment_id",
    ]
    casts = {
        "timestamp": "CAST({col} AS TIMESTAMPTZ)",
        "lat": "CAST({col} AS NUMERIC(9,6))",
        "lon": "CAST({col} AS NUMERIC(9,6))",
        "temperature_c": "NULLIF({col}, '')::NUMERIC(5,2)",
        "precip_mm_h": "NULLIF({col}, '')::NUMERIC(6,2)",
        "wind_kph": "NULLIF({col}, '')::NUMERIC(6,2)",
        "visibility_km": "NULLIF({col}, '')::NUMERIC(6,2)",
        "wx_condition": f"{config.DB_SCHEMA}.wx_enum({{col}})",
    }
    copy_with_temp(
        conn,
        path,
        "weather",
        cols,
        casts=casts,
        key_cols=["station_id", "timestamp"],
        upsert=True,
    )


def import_truck_profiles(conn: Connection, data_dir: Path) -> None:
    path = file_in(data_dir, "truck_profiles.csv")
    if not path:
        return
    cols = ["truck_id", "max_weight_tons", "height_m", "width_m", "hazmat"]
    casts = {
        "max_weight_tons": "NULLIF({col}, '')::NUMERIC(6,2)",
        "height_m": "NULLIF({col}, '')::NUMERIC(5,2)",
        "width_m": "NULLIF({col}, '')::NUMERIC(5,2)",
        "hazmat": "NULLIF({col}, '')::INT",
    }
    copy_with_temp(
        conn,
        path,
        "truck_profiles",
        cols,
        casts=casts,
        key_cols=["truck_id"],
        upsert=True,
    )


def import_deliveries(conn: Connection, data_dir: Path) -> None:
    path = file_in(data_dir, "deliveries.csv")
    if not path:
        return
    cols = [
        "delivery_id",
        "truck_id",
        "scheduled_departure_utc",
        "scheduled_arrival_utc",
        "origin_name",
        "origin_lat",
        "origin_lon",
        "destination_name",
        "destination_lat",
        "destination_lon",
        "priority",
        "commodity",
        "per_km_cost_rand",
        "per_hour_cost_rand",
    ]
    casts = {
        "scheduled_departure_utc": "NULLIF({col}, '')::TIMESTAMPTZ",
        "scheduled_arrival_utc": "NULLIF({col}, '')::TIMESTAMPTZ",
        "origin_lat": "NULLIF({col}, '')::NUMERIC(9,6)",
        "origin_lon": "NULLIF({col}, '')::NUMERIC(9,6)",
        "destination_lat": "NULLIF({col}, '')::NUMERIC(9,6)",
        "destination_lon": "NULLIF({col}, '')::NUMERIC(9,6)",
        "priority": f"{config.DB_SCHEMA}.priority_enum({{col}})",
        "per_km_cost_rand": "NULLIF({col}, '')::NUMERIC(10,2)",
        "per_hour_cost_rand": "NULLIF({col}, '')::NUMERIC(10,2)",
    }
    copy_with_temp(
        conn,
        path,
        "deliveries",
        cols,
        casts=casts,
        key_cols=["delivery_id"],
        upsert=True,
    )


def import_assignments(conn: Connection, data_dir: Path) -> None:
    path = file_in(data_dir, "assignments.csv")
    if not path:
        return
    df = pd.read_csv(path)
    if "route_segments" in df.columns:
        df["route_segments"] = ensure_json(df["route_segments"])
        tmp = Path(tempfile.mkstemp(suffix=".csv")[1])
        df.to_csv(tmp, index=False)
        path = tmp

    cols = [
        "assignment_id",
        "delivery_id",
        "planned_departure_utc",
        "planned_arrival_utc",
        "planned_distance_km",
        "planned_duration_min",
        "route_segments",
        "status",
        "reason",
    ]
    casts = {
        "planned_departure_utc": "NULLIF({col}, '')::TIMESTAMPTZ",
        "planned_arrival_utc": "NULLIF({col}, '')::TIMESTAMPTZ",
        "planned_distance_km": "NULLIF({col}, '')::NUMERIC(8,1)",
        "planned_duration_min": "NULLIF({col}, '')::INT",
        "route_segments": "NULLIF({col}, '')::JSONB",
        "status": f"{config.DB_SCHEMA}.assign_status_enum({{col}})",
    }
    copy_with_temp(
        conn,
        path,
        "assignments",
        cols,
        casts=casts,
        key_cols=["assignment_id"],
        upsert=True,
    )

    try:
        if "tmp" in str(path):
            os.remove(path)
    except Exception:
        pass


# -------- main --------
def main() -> None:
    data_dir = Path(config.DATA_DIR).expanduser().resolve()
    if not data_dir.exists():
        raise SystemExit(f"DATA_DIR not found: {data_dir}")

    eng = config.get_engine()
    with eng.begin() as conn:
        conn.execute(text(f"SET search_path = {config.DB_SCHEMA}, public"))

        steps = [
            ("roads", import_roads),
            ("truck_profiles", import_truck_profiles),
            ("deliveries", import_deliveries),
            ("assignments", import_assignments),
            ("historical_speeds", import_historical_speeds),
            ("incidents", import_incidents),
            ("weather", import_weather),
        ]

        for name, fn in steps:
            p = file_in(data_dir, f"{name}.csv")
            if p:
                print(f"-> importing {name} from {p.name}")
                fn(conn, data_dir)
            else:
                print(f"-> skipped {name}: {name}.csv not found")

    print("✅ Import finished.")


if __name__ == "__main__":
    main()
