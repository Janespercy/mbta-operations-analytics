"""Turn the query results in data/results/ into PNG charts for the README."""
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # write files only, no window
import matplotlib.pyplot as plt
import pandas as pd

RESULTS = Path("data/results")
IMAGES = Path("docs/images")
PERIODS = ["early_morning", "am_peak", "midday", "pm_peak", "evening", "late_night"]
PERIOD_LABELS = ["Early\nmorning", "AM\npeak", "Midday", "PM\npeak", "Evening", "Late\nnight"]
SCHEDULED, ADDED = "#4C72B0", "#DD8452"

plt.rcParams.update({
    "font.size": 10,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.titleweight": "bold",
    "axes.titlesize": 12,
})


def save(fig, name):
    fig.tight_layout()
    fig.savefig(IMAGES / name, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"saved {IMAGES / name}")


def headway_heatmap():
    df = pd.read_csv(RESULTS / "03_headway_adherence.csv")
    routes = list(dict.fromkeys(df["route_name"]))  # keep the query's route order
    grid = df.pivot(index="route_name", columns="time_period", values="pct_within_target")
    grid = grid.reindex(index=routes, columns=PERIODS)

    fig, ax = plt.subplots(figsize=(8, 0.5 * len(routes) + 1.5))
    im = ax.imshow(grid.values, cmap="RdYlGn", vmin=60, vmax=100, aspect="auto")
    for i in range(grid.shape[0]):
        for j in range(grid.shape[1]):
            value = grid.iat[i, j]
            if pd.notna(value):
                ax.text(j, i, f"{value:.0f}%", ha="center", va="center", fontsize=9)

    ax.set_xticks(range(len(PERIODS)), PERIOD_LABELS)
    ax.set_yticks(range(len(routes)), routes)
    ax.tick_params(length=0)
    for side in ax.spines.values():
        side.set_visible(False)
    ax.set_title("Headways within 1.5x of schedule, by route and time of day")
    fig.colorbar(im, ax=ax, shrink=0.8, label="% of headways")
    save(fig, "headway_adherence.png")


def schedule_mismatch():
    df = pd.read_csv(RESULTS / "01_schedule_mismatch_breakdown.csv")
    df["is_added_trip"] = df["is_added_trip"].astype(str).str.lower() == "true"
    overall = df.groupby("is_added_trip")[["mismatches", "events"]].sum()
    pct = 100 * overall["mismatches"] / overall["events"]

    routes = list(dict.fromkeys(df["route_name"]))
    wide = df.pivot(index="route_name", columns="is_added_trip", values="pct_mismatch").reindex(routes)

    fig, ax = plt.subplots(figsize=(8, 4.5))
    y = range(len(routes))
    ax.barh([i + 0.2 for i in y], wide[False], height=0.4, color=SCHEDULED, label="Scheduled trips")
    ax.barh([i - 0.2 for i in y], wide[True], height=0.4, color=ADDED, label="ADDED (unscheduled) trips")
    ax.set_yticks(list(y), routes)
    ax.invert_yaxis()
    ax.set_xlabel("% of events where the schedule match is off by more than 1 hour")
    ax.set_title(
        f"Schedule mismatches: {pct[True]:.1f}% of ADDED-trip events vs {pct[False]:.2f}% of scheduled"
    )
    ax.set_xlim(0, 100)
    ax.legend(frameon=False, loc="upper center", bbox_to_anchor=(0.5, -0.15), ncol=2)
    save(fig, "schedule_mismatch.png")


def hourly_profile():
    df = pd.read_csv(RESULTS / "04_hourly_profile_rolling.csv")
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(df["hour"], df["avg_events_per_day"], color="#C9D6E8", label="Avg stop events per day")
    ax.plot(df["hour"], df["rolling_3h_avg"], color=SCHEDULED, linewidth=2, label="3-hour rolling average")
    ax.set_xticks(range(0, 24, 2), [f"{h:02d}:00" for h in range(0, 24, 2)], rotation=45)
    ax.set_xlabel("Hour of day (hours 0-2 are after-midnight service)")
    ax.set_ylabel("Stop events")
    ax.set_title("Subway service through the day (average over Nov 15-21, 2026)")
    ax.legend(frameon=False, loc="upper center", bbox_to_anchor=(0.5, -0.3), ncol=2)
    save(fig, "hourly_profile.png")


def top_dwell():
    df = pd.read_csv(RESULTS / "05_top_dwell_stations_per_route.csv")
    # the query keeps each route's top 3 stations, so this is the top 12 of those
    top = df.sort_values("median_dwell_s", ascending=False).head(12)
    labels = top["station_name"] + " (" + top["route_name"].str.replace(" Line", "") + ")"

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.barh(labels, top["median_dwell_s"], color=SCHEDULED)
    ax.invert_yaxis()
    for i, v in enumerate(top["median_dwell_s"]):
        ax.text(v + 1, i, f"{v:.0f}s", va="center", fontsize=9)
    ax.set_xlabel("Median dwell time (seconds)")
    ax.set_title("Longest median dwell times (from each route's top 3 stations)")
    save(fig, "top_dwell.png")


if __name__ == "__main__":
    IMAGES.mkdir(parents=True, exist_ok=True)
    headway_heatmap()
    schedule_mismatch()
    hourly_profile()
    top_dwell()