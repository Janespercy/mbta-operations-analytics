"""Run every query in sql/ against the warehouse tables and save the results as CSV."""
import csv
from pathlib import Path

from pyspark.sql import SparkSession

WAREHOUSE = "data/warehouse"
TABLES = ["fact_stop_event", "dim_route", "dim_stop", "dim_date", "dim_time", "rejected_events"]
RESULTS = Path("data/results")


def save_csv(df, path):
    # write via the csv module instead of toPandas() to avoid the pandas version check
    rows = df.collect()
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(df.columns)
        writer.writerows(rows)


def main():
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("mbta-queries")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    for table in TABLES:
        spark.read.parquet(f"{WAREHOUSE}/{table}").createOrReplaceTempView(table)

    RESULTS.mkdir(parents=True, exist_ok=True)
    for path in sorted(Path("sql").glob("*.sql")):
        df = spark.sql(path.read_text())
        print(f"\n== {path.stem} ==")
        df.show(30, truncate=False)
        save_csv(df, RESULTS / f"{path.stem}.csv")

    spark.stop()


if __name__ == "__main__":
    main()
