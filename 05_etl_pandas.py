"""Pandas version of 03_etl_spark.py: same star schema and data quality rules, no Spark.

Writes to data/warehouse_pandas/ so the two outputs can be compared (06_compare_etl.py).
"""
import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

RAW = Path("data/raw")
OUT = Path("data/warehouse_pandas")
TZ = "America/New_York"

# keep these in sync with 03_etl_spark.py -- 06_compare_etl.py will catch it if they drift
MAX_TRAVEL = 30 * 60
MAX_DWELL = 30 * 60
MAX_HEADWAY = 2 * 60 * 60
MAX_SCHEDULE_DIFF = 60 * 60
EVENT_KEY = ["service_date", "trip_id", "stop_sequence"]
SOFT_RULES = {
    "travel_time_seconds": (1, MAX_TRAVEL),
    "dwell_time_seconds": (0, MAX_DWELL),
    "headway_trunk_seconds": (1, MAX_HEADWAY),
    "headway_branch_seconds": (1, MAX_HEADWAY),
    "arrival_delay_seconds": (-MAX_SCHEDULE_DIFF, MAX_SCHEDULE_DIFF),
}
FACT_COLUMNS = [
    "date_key", "time_key", "route_id", "stop_id", "trip_id", "stop_sequence", "vehicle_id",
    "direction_id", "direction", "direction_destination", "trunk_route_id", "branch_route_id",
    "is_added_trip", "is_schedule_mismatch", "car_count", "move_ts", "stop_ts",
    "travel_time_seconds", "dwell_time_seconds", "headway_trunk_seconds", "headway_branch_seconds",
    "arrival_delay_seconds", "scheduled_travel_time", "scheduled_headway_trunk", "scheduled_headway_branch",
]


def latest_version(df, key):
    return df.sort_values("static_version_key", ascending=False, kind="stable").drop_duplicates(key)


def load_events():
    files = sorted((RAW / "otp").glob("*.parquet"))
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def build_dim_route(events):
    routes = pd.read_parquet(
        RAW / "static/LAMP_static_routes.parquet",
        filters=[("route_id", "in", events["route_id"].dropna().unique().tolist())],
    )
    routes = latest_version(routes, "route_id").rename(
        columns={"route_long_name": "route_name", "route_desc": "route_category"}
    )
    return routes[["route_id", "route_name", "line_id", "route_category", "route_type", "route_sort_order"]]


def build_dim_stop(events):
    platform_ids = set(events["stop_id"].dropna().unique())
    needed = platform_ids | set(events["parent_station"].dropna().unique())
    # filter while reading (the pandas equivalent of the semi-join) instead of loading all 7.6M rows
    stops = pd.read_parquet(
        RAW / "static/LAMP_static_stops.parquet",
        columns=["stop_id", "stop_name", "platform_name", "stop_desc", "parent_station", "static_version_key"],
        filters=[("stop_id", "in", sorted(needed))],
    )
    stops = latest_version(stops, "stop_id")
    stations = stops[["stop_id", "stop_name"]].rename(
        columns={"stop_id": "parent_station", "stop_name": "station_name"}
    )
    platforms = stops[stops["stop_id"].isin(platform_ids)]
    return platforms.merge(stations, on="parent_station", how="left")[
        ["stop_id", "stop_name", "platform_name", "stop_desc", "parent_station", "station_name"]
    ]


def build_dim_date(events):
    keys = np.sort(events["service_date"].unique())
    d = pd.to_datetime(pd.Series(keys).astype(str), format="%Y%m%d")
    return pd.DataFrame({
        "date_key": keys.astype("int32"),
        "date": d.dt.date,
        "day_of_week": (d.dt.dayofweek + 1) % 7 + 1,  # match Spark: 1 = Sunday
        "day_name": d.dt.day_name(),
        "is_weekend": d.dt.dayofweek >= 5,
    })


def build_dim_time():
    minutes = np.arange(24 * 60)
    hour = minutes // 60
    period = np.select(
        [hour < 6, hour < 9, hour < 15, hour < 19, hour < 22],
        ["early_morning", "am_peak", "midday", "pm_peak", "evening"],
        default="late_night",
    )
    return pd.DataFrame({
        "time_key": minutes.astype("int32"),
        "hour": hour.astype("int32"),
        "minute": (minutes % 60).astype("int32"),
        "time_period": period,
    })


def add_derived_columns(ev):
    ts = pd.to_datetime(ev["stop_timestamp"].fillna(ev["move_timestamp"]), unit="s", utc=True).dt.tz_convert(TZ)
    ev["time_key"] = ts.dt.hour * 60 + ts.dt.minute

    midnight = {
        d: pd.to_datetime(str(d), format="%Y%m%d").tz_localize(TZ).timestamp()
        for d in ev["service_date"].unique()
    }
    ev["arrival_delay_seconds"] = (
        ev["stop_timestamp"] - ev["service_date"].map(midnight) - ev["scheduled_arrival_time"]
    )
    ev["is_added_trip"] = ev["trip_id"].str.startswith("ADDED-")
    ev["car_count"] = (ev["vehicle_consist"].str.count(r"\|") + 1).astype("Int64")
    return ev


def apply_hard_rules(ev, dim_route, dim_stop):
    # same ordering as the Spark window: earliest arrival wins, null arrivals last
    ev = ev.sort_values(EVENT_KEY + ["stop_timestamp"], na_position="last", kind="stable")
    conditions = [
        ev["trip_id"].isna() | ev["stop_id"].isna() | ev["route_id"].isna(),
        ev["stop_timestamp"].isna() & ev["move_timestamp"].isna(),
        ev.duplicated(EVENT_KEY, keep="first"),
        ~ev["route_id"].isin(dim_route["route_id"]),
        ~ev["stop_id"].isin(dim_stop["stop_id"]),
    ]
    reasons = ["missing_key", "missing_timestamps", "conflicting_event_key", "unknown_route", "unknown_stop"]
    ev["reject_reason"] = np.select(conditions, reasons, default=None)
    return ev


def apply_soft_rules(valid):
    counts = {
        c: int((valid[c].notna() & ~valid[c].between(lo, hi).fillna(False)).sum())
        for c, (lo, hi) in SOFT_RULES.items()
    }
    delay = valid["arrival_delay_seconds"]
    valid["is_schedule_mismatch"] = delay.notna() & ~delay.between(-MAX_SCHEDULE_DIFF, MAX_SCHEDULE_DIFF).fillna(False)
    for c, (lo, hi) in SOFT_RULES.items():
        valid[c] = valid[c].where(valid[c].between(lo, hi).fillna(False))
    return valid, counts


def build_fact(valid):
    fact = valid.assign(
        date_key=valid["service_date"].astype("int32"),
        time_key=valid["time_key"].astype("int32"),
        move_ts=pd.to_datetime(valid["move_timestamp"], unit="s", utc=True),
        stop_ts=pd.to_datetime(valid["stop_timestamp"], unit="s", utc=True),
    )
    return fact[FACT_COLUMNS]


def write_table(df, name, partition_cols=None):
    # one folder per table, same layout as the Spark warehouse
    path = OUT / name
    shutil.rmtree(path, ignore_errors=True)
    if partition_cols:
        df.to_parquet(path, partition_cols=partition_cols, index=False)
    else:
        path.mkdir(parents=True)
        df.to_parquet(path / "part-0.parquet", index=False)


def main():
    events = load_events()
    raw_rows = len(events)

    dim_route = build_dim_route(events)
    dim_stop = build_dim_stop(events)
    dim_date = build_dim_date(events)
    dim_time = build_dim_time()

    tagged = apply_hard_rules(add_derived_columns(events), dim_route, dim_stop)
    rejected = tagged[tagged["reject_reason"].notna()]
    valid, soft_counts = apply_soft_rules(tagged[tagged["reject_reason"].isna()].copy())
    fact = build_fact(valid)

    for name, df in [("dim_route", dim_route), ("dim_stop", dim_stop), ("dim_date", dim_date),
                     ("dim_time", dim_time), ("rejected_events", rejected)]:
        write_table(df, name)
    write_table(fact, "fact_stop_event", partition_cols=["date_key"])

    summary = {
        "raw_rows": raw_rows,
        "rejected_rows": {k: int(v) for k, v in rejected["reject_reason"].value_counts().items()},
        "fact_rows": len(fact),
        "pct_rows_kept": round(100 * len(fact) / raw_rows, 2),
        "values_nulled_out_of_range": soft_counts,
        "dim_rows": {"dim_route": len(dim_route), "dim_stop": len(dim_stop),
                     "dim_date": len(dim_date), "dim_time": len(dim_time)},
    }
    (OUT / "dq_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()