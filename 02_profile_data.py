"""Quick profile of the raw LAMP files: schema, row counts, nulls, sample rows."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW = "data/raw"


def null_report(df, n_rows):
    counts = df.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
    ).first()
    for col in df.columns:
        n = counts[col] or 0
        print(f"  {col:<40} {n:>10,} ({n / max(n_rows, 1):.1%})")


def categorical_report(df, max_values=30):
    for col, dtype in df.dtypes:
        if dtype not in ("string", "boolean"):
            continue
        values = [r[0] for r in df.select(col).distinct().limit(max_values + 1).collect()]
        if len(values) <= max_values:
            print(f"  {col}: {values}")
        else:
            print(f"  {col}: more than {max_values} distinct values")


def profile(df, name):
    print(f"\n--- {name} ---")
    df.printSchema()
    n = df.count()
    print(f"{n:,} rows, {len(df.columns)} columns\n")
    print("nulls:")
    null_report(df, n)
    print("\nlow-cardinality columns:")
    categorical_report(df)
    df.show(2, truncate=False, vertical=True)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("mbta-profile")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # mergeSchema in case columns changed between daily files
    events = spark.read.option("mergeSchema", "true").parquet(f"{RAW}/otp/*.parquet")
    profile(events, "subway events")

    print("rows per daily file:")
    (events.groupBy(F.col("_metadata.file_path").alias("file"))
        .count().orderBy("file").show(truncate=False))

    for table in ["routes", "stops"]:
        profile(spark.read.parquet(f"{RAW}/static/LAMP_static_{table}.parquet"), table)

    spark.stop()