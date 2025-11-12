# incidents_feeder.py
from __future__ import annotations

# --- load local app_config.py next to this file ---
from pathlib import Path
from importlib.util import spec_from_file_location, module_from_spec

CFG_PATH = Path(__file__).with_name("app_config.py")
spec = spec_from_file_location("app_config", str(CFG_PATH))
config = module_from_spec(spec)
spec.loader.exec_module(config)  # type: ignore
# --------------------------------------------------

import random, time
from datetime import datetime, timezone
from typing import Dict, List, Tuple
from sqlalchemy import text

# -------- settings (tweak for your demo) --------
TICK_SECONDS = 7  # insert every N seconds (pick 5–20)
BATCH_RANGE = (1, 3)  # create 1..3 incidents per tick
# ------------------------------------------------


def midpoint(lat1: float, lon1: float, lat2: float, lon2: float) -> Tuple[float, float]:
    return round((lat1 + lat2) / 2.0, 6), round((lon1 + lon2) / 2.0, 6)


def load_roads(conn) -> List[Dict]:
    rows = (
        conn.execute(
            text(
                """
        SELECT segment_id, from_lat, from_lon, to_lat, to_lon,
               municipality, province
        FROM gauteng.roads
    """
            )
        )
        .mappings()
        .all()
    )
    if not rows:
        raise SystemExit("No rows in gauteng.roads — import roads first.")
    return list(rows)


def synth_incident(seg: Dict) -> Dict:
    now = datetime.now(timezone.utc)
    inc_type = random.choices(
        ["accident", "roadwork", "closure", "hazard"], weights=[55, 20, 5, 20], k=1
    )[0]
    desc = {
        "accident": "Two-vehicle collision reported by motorists",
        "roadwork": "Maintenance crew working on lane resurfacing",
        "closure": "Temporary closure due to local event",
        "hazard": "Debris on road — caution",
    }[inc_type]
    lat, lon = midpoint(
        float(seg["from_lat"]),
        float(seg["from_lon"]),
        float(seg["to_lat"]),
        float(seg["to_lon"]),
    )
    return dict(
        incident_id=f"INC-{now.strftime('%Y%m%d%H%M%S')}-{random.randint(1000,9999)}",
        ts=now,
        itype=inc_type,
        severity=random.randint(1, 5),
        lat=lat,
        lon=lon,
        seg_id=seg["segment_id"],
        desc=desc,
        src="simulator",
    )


def main():
    eng = config.get_engine()
    with eng.begin() as c:
        c.execute(text(f"SET search_path = {config.DB_SCHEMA}, public"))
        roads = load_roads(c)
    print(
        f"Loaded {len(roads)} road segments. Inserting every {TICK_SECONDS}s … (Ctrl+C to stop)"
    )

    try:
        while True:
            batch = random.randint(*BATCH_RANGE)
            payload = [synth_incident(random.choice(roads)) for _ in range(batch)]
            with eng.begin() as c:
                c.execute(text(f"SET search_path = {config.DB_SCHEMA}, public"))
                c.execute(
                    text(
                        """
                    INSERT INTO gauteng.incidents(
                      incident_id, "timestamp", type, severity, lat, lon,
                      affected_segment_id, description, source
                    )
                    VALUES (:incident_id, :ts, :itype, :severity, :lat, :lon,
                            :seg_id, :desc, :src)
                    ON CONFLICT (incident_id) DO NOTHING
                """
                    ),
                    payload,
                )
            print(f"[{datetime.now().strftime('%H:%M:%S')}] +{len(payload)} incidents")
            time.sleep(TICK_SECONDS)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
