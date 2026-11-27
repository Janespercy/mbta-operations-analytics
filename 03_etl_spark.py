"""Build a star schema from raw MBTA LAMP subway events with PySpark.

Reads data/raw/, writes Parquet tables plus a data quality summary to data/warehouse/.
"""
import json
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

RAW = "data/raw"
OUT = "data/warehouse"

# Plausibility bounds in seconds. Values outside these are treated as bad sensor readings.
MAX_TRAVEL = 30 * 60
MAX_DWELL = 30 * 60
MAX_HEADWAY = 2 * 60 * 60
# Past this, the schedule match is probably wrong (ADDED trips get matched to unrelated scheduled times)
MAX_SCHEDULE_DIFF = 60 * 60

EVENT_KEY = ["service_date", "trip_id", "stop_sequence"]


def latest_version(df, key):
    # static tables keep a copy of every row for every GTFS version; keep the newest per key
    w = Window.partitionBy(key).orderBy(F.col("static_version_key").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def build_dim_route(spark, events):
    routes = spark.read.parquet(f"{RAW}/static/LAMP_static_routes.parquet")
    used = events.select("route_id").distinct()
    routes = latest_version(routes.join(F.broadcast(used), "route_id", "left_semi"), "route_id")
    return routes.select(
        "route_id",
        F.col("route_long_name").alias("route_name"),
        "line_id",
        F.col("route_desc").alias("route_category"),
        "route_type",
        "route_sort_order",
    )


def build_dim_stop(spark, events):
    # the stops file is 7.6M rows (every stop, every mode, every version), so cut it down
    # to the platforms and parent stations that show up in the events before the window
    needed = (
        events.select("stop_id")
        .union(events.select(F.col("parent_station").alias("stop_id")))
        .distinct()
    )
    stops = spark.read.parquet(f"{RAW}/static/LAMP_static_stops.parquet")
    stops = latest_version(stops.join(F.broadcast(needed), "stop_id", "left_semi"), "stop_id")

    stations = stops.select(
        F.col("stop_id").alias("parent_station"), F.col("stop_name").alias("station_name")
    )
    platforms = stops.join(events.select("stop_id").distinct(), "stop_id", "left_semi")
    return platforms.join(stations, "parent_station", "left").select(
        "stop_id", "stop_name", "platform_name", "stop_desc", "parent_station", "station_name"
    )


def build_dim_date(events):
    d = F.to_date(F.col("service_date").cast("string"), "yyyyMMdd")
    return events.select("service_date").distinct().select(
        F.col("service_date").cast("int").alias("date_key"),
        d.alias("date"),
        F.dayofweek(d).alias("day_of_week"),  # 1 = Sunday
        F.date_format(d, "EEEE").alias("day_name"),
        F.dayofweek(d).isin(1, 7).alias("is_weekend"),
    )


def build_dim_time(spark):
    # one row per minute of the day; time periods are this project's own definitions
    hour = F.floor(F.col("id") / 60)
    return spark.range(24 * 60).select(
        F.col("id").cast("int").alias("time_key"),
        hour.cast("int").alias("hour"),
        (F.col("id") % 60).cast("int").alias("minute"),
        F.when(hour < 6, "early_morning")
        .when(hour < 9, "am_peak")
        .when(hour < 15, "midday")
        .when(hour < 19, "pm_peak")
        .when(hour < 22, "evening")
        .otherwise("late_night")
        .alias("time_period"),
    )


def add_derived_columns(events):
    event_ts = F.timestamp_seconds(F.coalesce("stop_timestamp", "move_timestamp"))
    # local midnight of the service date; scheduled_* columns are seconds after this
    # (GTFS technically uses noon minus 12h, which only differs on DST change days)
    service_midnight = F.unix_timestamp(F.to_timestamp(F.col("service_date").cast("string"), "yyyyMMdd"))

    return (
        events.withColumn("time_key", F.hour(event_ts) * 60 + F.minute(event_ts))
        .withColumn("arrival_delay_seconds", F.col("stop_timestamp") - service_midnight - F.col("scheduled_arrival_time"))
        .withColumn("is_added_trip", F.col("trip_id").startswith("ADDED-"))
        .withColumn(
            "car_count",
            F.when(F.col("vehicle_consist").isNotNull(), F.size(F.split("vehicle_consist", r"\|"))),
        )
    )


def apply_hard_rules(events, dim_route, dim_stop):
    """Tag rows that can't go in the fact table. First failing rule wins."""
    w = Window.partitionBy(*EVENT_KEY).orderBy(F.col("stop_timestamp").asc_nulls_last())
    tagged = (
        events.withColumn("_dup_rank", F.row_number().over(w))
        .join(dim_route.select("route_id", F.lit(True).alias("_known_route")), "route_id", "left")
        .join(dim_stop.select("stop_id", F.lit(True).alias("_known_stop")), "stop_id", "left")
    )
    reason = (
        F.when(F.col("trip_id").isNull() | F.col("stop_id").isNull() | F.col("route_id").isNull(), "missing_key")
        .when(F.col("stop_timestamp").isNull() & F.col("move_timestamp").isNull(), "missing_timestamps")
        # same (service_date, trip_id, stop_sequence) seen more than once; mostly NOT exact copies
        # (different arrival times / stops), so these are key conflicts, not true duplicates
        .when(F.col("_dup_rank") > 1, "conflicting_event_key")
        .when(F.col("_known_route").isNull(), "unknown_route")
        .when(F.col("_known_stop").isNull(), "unknown_stop")
    )
    return tagged.withColumn("reject_reason", reason).drop("_dup_rank", "_known_route", "_known_stop")


SOFT_RULES = {
    # column: (low, high) -- out-of-range values get nulled, the row stays
    "travel_time_seconds": (1, MAX_TRAVEL),
    "dwell_time_seconds": (0, MAX_DWELL),
    "headway_trunk_seconds": (1, MAX_HEADWAY),
    "headway_branch_seconds": (1, MAX_HEADWAY),
    "arrival_delay_seconds": (-MAX_SCHEDULE_DIFF, MAX_SCHEDULE_DIFF),
}


def apply_soft_rules(valid):
    flags = {
        c: F.sum((F.col(c).isNotNull() & ~F.col(c).between(lo, hi)).cast("int")).alias(c)
        for c, (lo, hi) in SOFT_RULES.items()
    }
    counts = valid.select(*flags.values()).first().asDict()
    # keep a flag for delay so we can see later which trips the schedule match fails on
    valid = valid.withColumn(
        "is_schedule_mismatch",
        F.col("arrival_delay_seconds").isNotNull()
        & ~F.col("arrival_delay_seconds").between(-MAX_SCHEDULE_DIFF, MAX_SCHEDULE_DIFF),
    )
    for c, (lo, hi) in SOFT_RULES.items():
        valid = valid.withColumn(c, F.when(F.col(c).between(lo, hi), F.col(c)))
    return valid, {k: v or 0 for k, v in counts.items()}


def build_fact(valid):
    return valid.select(
        F.col("service_date").cast("int").alias("date_key"),
        "time_key",
        "route_id",
        "stop_id",
        "trip_id",
        "stop_sequence",
        "vehicle_id",
        "direction_id",
        "direction",
        "direction_destination",
        "trunk_route_id",
        "branch_route_id",
        "is_added_trip",
        "is_schedule_mismatch",
        "car_count",
        F.timestamp_seconds("move_timestamp").alias("move_ts"),
        F.timestamp_seconds("stop_timestamp").alias("stop_ts"),
        "travel_time_seconds",
        "dwell_time_seconds",
        "headway_trunk_seconds",
        "headway_branch_seconds",
        "arrival_delay_seconds",
        "scheduled_travel_time",
        "scheduled_headway_trunk",
        "scheduled_headway_branch",
    )


def main():
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("mbta-etl")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.shuffle.partitions", "8")  # default 200 is overkill for ~300K rows
        .config("spark.driver.memory", "2g")  # 1g default was tight when writing 8 partitions
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    events = spark.read.option("mergeSchema", "true").parquet(f"{RAW}/otp/*.parquet").cache()
    raw_rows = events.count()

    dim_route = build_dim_route(spark, events).cache()
    dim_stop = build_dim_stop(spark, events).cache()
    dim_date = build_dim_date(events)
    dim_time = build_dim_time(spark)

    tagged = apply_hard_rules(add_derived_columns(events), dim_route, dim_stop).cache()
    rejected = tagged.filter(F.col("reject_reason").isNotNull())
    valid, soft_counts = apply_soft_rules(tagged.filter(F.col("reject_reason").isNull()))
    fact = build_fact(valid)

    reject_counts = {r["reject_reason"]: r["count"] for r in rejected.groupBy("reject_reason").count().collect()}
    valid_rows = raw_rows - sum(reject_counts.values())

    dim_route.write.mode("overwrite").parquet(f"{OUT}/dim_route")
    dim_stop.write.mode("overwrite").parquet(f"{OUT}/dim_stop")
    dim_date.write.mode("overwrite").parquet(f"{OUT}/dim_date")
    dim_time.write.mode("overwrite").parquet(f"{OUT}/dim_time")
    fact.write.mode("overwrite").partitionBy("date_key").parquet(f"{OUT}/fact_stop_event")
    rejected.write.mode("overwrite").parquet(f"{OUT}/rejected_events")

    summary = {
        "raw_rows": raw_rows,
        "rejected_rows": reject_counts,
        "fact_rows": valid_rows,
        "pct_rows_kept": round(100 * valid_rows / raw_rows, 2),
        "values_nulled_out_of_range": soft_counts,
        "dim_rows": {
            "dim_route": dim_route.count(),
            "dim_stop": dim_stop.count(),
            "dim_date": dim_date.count(),
            "dim_time": dim_time.count(),
        },
    }
    Path(OUT).mkdir(parents=True, exist_ok=True)
    Path(f"{OUT}/dq_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))

    spark.stop()


if __name__ == "__main__":
    main()