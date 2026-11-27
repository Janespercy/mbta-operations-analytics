"""Run the Spark and pandas ETLs, time them, and check that they produce the same results."""
import argparse
import json
import statistics
import subprocess
import sys
import time

import pandas as pd

IMPLEMENTATIONS = {
    "spark": ("03_etl_spark.py", "data/warehouse"),
    "pandas": ("05_etl_pandas.py", "data/warehouse_pandas"),
}


def timed_run(script):
    start = time.perf_counter()
    result = subprocess.run([sys.executable, script], capture_output=True, text=True)
    elapsed = time.perf_counter() - start
    if result.returncode != 0:
        print(result.stderr[-3000:])
        sys.exit(f"{script} failed")
    return elapsed


def fact_profile(warehouse):
    # per day and route: row counts plus sums of the main measures
    fact = pd.read_parquet(f"{warehouse}/fact_stop_event")
    fact["date_key"] = fact["date_key"].astype("int64")
    return (
        fact.groupby(["date_key", "route_id"])
        .agg(
            rows=("stop_id", "size"),
            travel_sum=("travel_time_seconds", "sum"),
            dwell_sum=("dwell_time_seconds", "sum"),
            headway_sum=("headway_trunk_seconds", "sum"),
            delays=("arrival_delay_seconds", "count"),
            mismatches=("is_schedule_mismatch", "sum"),
            time_key_sum=("time_key", "sum"),
        )
        .astype("float64")
        .sort_index()
    )


def main(runs):
    times = {}
    for name, (script, _) in IMPLEMENTATIONS.items():
        times[name] = [timed_run(script) for _ in range(runs)]
        print(f"{name:<7} runs (s): {[round(t, 1) for t in times[name]]}")

    print("\nmedian end-to-end runtime:")
    for name, t in times.items():
        print(f"  {name:<7} {statistics.median(t):.1f} s")

    summaries = {
        name: json.load(open(f"{wh}/dq_summary.json")) for name, (_, wh) in IMPLEMENTATIONS.items()
    }
    print("\ndata quality summaries match:", summaries["spark"] == summaries["pandas"])
    if summaries["spark"] != summaries["pandas"]:
        for key in summaries["spark"]:
            if summaries["spark"][key] != summaries["pandas"].get(key):
                print(f"  {key}:\n    spark  {summaries['spark'][key]}\n    pandas {summaries['pandas'].get(key)}")

    spark_p = fact_profile(IMPLEMENTATIONS["spark"][1])
    pandas_p = fact_profile(IMPLEMENTATIONS["pandas"][1])
    joined = spark_p.join(pandas_p, lsuffix="_spark", rsuffix="_pandas", how="outer")
    cols = spark_p.columns
    diffs = sum(
        int(((joined[f"{c}_spark"] - joined[f"{c}_pandas"]).abs() > 1e-6).sum()
            + (joined[f"{c}_spark"].isna() != joined[f"{c}_pandas"].isna()).sum())
        for c in cols
    )
    cells = len(joined) * len(cols)
    print(f"fact table profile: {cells - diffs} of {cells} (day, route, measure) values match")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=3)
    main(parser.parse_args().runs)