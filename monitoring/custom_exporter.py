# custom_exporter.py  (InSight Mars Weather)
import os, time, requests
from prometheus_client import start_http_server, Gauge

NASA_API_KEY = os.getenv("NASA_API_KEY", "DEMO_KEY")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "20"))
EXPORTER_PORT = int(os.getenv("EXPORTER_PORT", "9105"))

# --- core health ---
api_up = Gauge("mars_insight_api_up", "1=last call OK, 0=failed")
api_status = Gauge("mars_insight_http_status", "HTTP status of last call")
api_duration = Gauge("mars_insight_request_duration_seconds", "Duration of last call")
api_errors_total = Gauge("mars_insight_errors_total", "Total failed calls")
api_last_fetch_unix = Gauge(
    "mars_insight_last_fetch_unix", "Unix ts of last successful call"
)

# --- weather metrics (labeled) ---
# stat ∈ {av,mn,mx}
temp_c = Gauge("mars_insight_temp_celsius", "Air temperature (°C)", ["sol", "stat"])
wind_mps = Gauge("mars_insight_wind_mps", "Wind speed (m/s)", ["sol", "stat"])
pressure_pa = Gauge("mars_insight_pressure_pa", "Pressure (Pa)", ["sol", "stat"])

# wind directions per sol (counts from histogram)
wind_dir_counts = Gauge(
    "mars_insight_wind_dir_counts", "Wind direction counts", ["sol", "dir", "deg"]
)

# season metadata (gauge set to 1 with labels carries the info)
season_info = Gauge(
    "mars_insight_season_info", "Season one-hot", ["sol", "season", "hemisphere"]
)

errors = 0


def set_triplet(gauge, sol, block, keys=("av", "mn", "mx")):
    if not isinstance(block, dict):
        return
    for k in keys:
        if k in block and block[k] is not None:
            gauge.labels(sol=sol, stat=k).set(float(block[k]))


def fetch_once():
    global errors
    url = f"https://api.nasa.gov/insight_weather/?api_key={NASA_API_KEY}&feedtype=json&ver=1.0"
    t0 = time.time()
    try:
        r = requests.get(url, timeout=15)
        api_duration.set(time.time() - t0)
        api_status.set(r.status_code)

        if r.status_code != 200:
            api_up.set(0)
            errors += 1
            api_errors_total.set(errors)
            return

        data = r.json()
        sol_keys = data.get("sol_keys") or []
        if not sol_keys:
            api_up.set(0)
            return

        for sol in sol_keys:
            sol_block = data.get(sol, {})
            set_triplet(temp_c, sol, sol_block.get("AT", {}))
            set_triplet(wind_mps, sol, sol_block.get("HWS", {}))
            set_triplet(pressure_pa, sol, sol_block.get("PRE", {}))

            # season labels
            season = sol_block.get("Season") or "unknown"
            north = sol_block.get("Northern_season") or "unknown"
            season_info.labels(sol=sol, season=season, hemisphere=north).set(1)

            # wind direction histogram
            wd = sol_block.get("WD", {}) or {}
            for k, bucket in wd.items():
                if k == "most_common":
                    continue
                cp = bucket.get("compass_point")
                deg = bucket.get("compass_degrees")
                ct = bucket.get("ct")
                if cp is not None and ct is not None and deg is not None:
                    wind_dir_counts.labels(sol=sol, dir=str(cp), deg=str(deg)).set(
                        float(ct)
                    )

        api_up.set(1)
        api_last_fetch_unix.set(time.time())

    except Exception:
        api_up.set(0)
        errors += 1
        api_errors_total.set(errors)


def main():
    print(
        f"[custom_exporter] InSight exporter on :{EXPORTER_PORT}, poll={POLL_SECONDS}s"
    )
    start_http_server(EXPORTER_PORT)
    while True:
        fetch_once()
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
