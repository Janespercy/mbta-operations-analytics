"""
Step 2: Profile the raw data with PySpark (local mode) BEFORE designing the schema.
Paste the full console output back to Claude.

Usage:  python 02_profile_data.py > profile_output.txt
"""
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder.master("local[*]")
    .appName("mbta-profile")
    .config("spark.sql.session.timeZone", "America/New_York")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def profile(df, name, max_distinct=30):
    print(f"\n{'=' * 70}\n{name}\n{'=' * 70}")
    df.printSchema()
    n = df.count()
    print(f"rows: {n:,}   columns: {len(df.columns)}")

    print("\nNull counts:")
    nulls = df.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
    ).first().asDict()
    for c, v in nulls.items():
        v = v or 0
        print(f"  {c:45s} {v:>12,}  ({v / max(n, 1):.2%})")

    print("\nLow-cardinality string/bool columns (candidate dimensions/flags):")
    for c, t in df.dtypes:
        if t in ("string", "boolean"):
            k = df.select(c).distinct().count()
            if k <= max_distinct:
                vals = [r[0] for r in df.select(c).distinct().collect()]
                print(f"  {c} ({k}): {vals}")
            else:
                print(f"  {c}: {k:,} distinct")

    print("\nSample rows:")
    df.show(2, truncate=False, vertical=True)


otp = spark.read.option("mergeSchema", "true").parquet("data/raw/otp/*.parquet")
otp = otp.withColumn("_source_file", F.input_file_name())
profile(otp, "FACT SOURCE: subway on-time performance events")
print("\nRows per source file:")
otp.groupBy("_source_file").count().orderBy("_source_file").show(truncate=False)

for name in ["routes", "stops"]:
    df = spark.read.parquet(f"data/raw/static/LAMP_static_{name}.parquet")
    profile(df, f"DIMENSION SOURCE: {name}")

spark.stop()