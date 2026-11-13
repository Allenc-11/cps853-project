"""Step 1: confirm Spark can read the raw file and report its shape.

Run from the project root (Windows PowerShell):
  docker run --rm --user root -v "${PWD}:/work" -w /work spark:python3 /opt/spark/bin/spark-submit --master "local[*]" jobs/explore.py
"""
from pyspark.sql import SparkSession, functions as F

RAW = "data/raw/household_power_consumption.txt"

spark = SparkSession.builder.appName("explore").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = (spark.read
      .option("sep", ";")
      .option("header", True)
      .option("nullValue", "?")
      .csv(RAW))

print("\n=== Schema ===")
df.printSchema()

print("=== First 5 rows ===")
df.show(5, truncate=False)

total = df.count()
print(f"=== Total rows: {total} ===")

print("=== Null count per column ===")
df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).show(truncate=False)

print("=== Rows where every measurement is null ===")
measure_cols = [c for c in df.columns if c not in ("Date", "Time")]
print(df.filter(F.coalesce(*[F.col(c) for c in measure_cols]).isNull()).count())

spark.stop()
