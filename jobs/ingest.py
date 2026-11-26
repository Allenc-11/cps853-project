"""Part 1a: convert the raw semicolon-separated text file to Parquet.

- Parses Date + Time into one timestamp column `ts` (stored as wall-clock time, session TZ = UTC).
- Casts the 7 measurement columns to double.
- Runs the Part 4 quality checks (quality/rules.py). Rows that fail are set aside in
  data/quarantine/ingest (not dropped), with the first check they failed as the reason.
- Prints rows in / rows out / rows rejected, the rows failed per check, any alert, and the size
  of the data before and after. The counts are also saved to data/quality/ingest.json, which
  serve/load_batch.py loads into PostgreSQL.

Run from the project root (Windows PowerShell):
  docker run --rm --user root -v "${PWD}:/work" -w /work spark:python3 /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g jobs/ingest.py
"""
import json
import os
import time

from pyspark.sql import SparkSession, Window, functions as F

from quality import rules

RAW = "data/raw/household_power_consumption.txt"
OUT = "data/parquet/power"
QUARANTINE = "data/quarantine/ingest"
QUALITY_REPORT = "data/quality/ingest.json"
TS_FORMAT = "d/M/yyyy H:mm:ss"

# raw column name -> clean column name
MEASURES = {
    "Global_active_power": "global_active_power",      # kW (minute average)
    "Global_reactive_power": "global_reactive_power",  # kW (minute average)
    "Voltage": "voltage",                              # V
    "Global_intensity": "global_intensity",            # A
    "Sub_metering_1": "sub_metering_1",                # Wh in the minute (kitchen)
    "Sub_metering_2": "sub_metering_2",                # Wh in the minute (laundry)
    "Sub_metering_3": "sub_metering_3",                # Wh in the minute (water heater + A/C)
}


def data_bytes(path):
    """Total size of data files under path, ignoring Spark/Hadoop metadata (_SUCCESS, .crc)."""
    if os.path.isfile(path):
        return os.path.getsize(path)
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            if name.startswith(("_", ".")):
                continue
            total += os.path.getsize(os.path.join(root, name))
    return total


def mb(n):
    return f"{n / 1024 / 1024:.1f} MB"


spark = (SparkSession.builder.appName("ingest")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
start = time.time()

raw = (spark.read
       .option("sep", ";")
       .option("header", True)
       .option("nullValue", "?")
       .csv(RAW))  # every column read as string; types are applied below

typed = raw.select(
    "Date",
    "Time",
    F.try_to_timestamp(F.concat_ws(" ", "Date", "Time"), F.lit(TS_FORMAT)).alias("ts"),
    *[F.col(src).try_cast("double").alias(dst) for src, dst in MEASURES.items()],
)

measure_cols = list(MEASURES.values())
P, Q, V, I, S1, S2, S3 = (F.col(c) for c in measure_cols)

# Part 4 checks, in the order of rules.BATCH_CHECKS. Each is a column that is true when the row
# fails; a comparison with a null is treated as passing, because missing values have their own check.
# The same limits are applied to the stream by rules.range_check().
# Keeps the first reading of each minute: the raw file's order decides which one that is.
first_of_minute = Window.partitionBy("ts").orderBy("_line")
failed = {
    "unparseable_timestamp": F.col("ts").isNull(),
    "all_measurements_missing": F.coalesce(*[F.col(c) for c in measure_cols]).isNull(),
    "missing_measurement": F.greatest(*[F.col(c).isNull() for c in measure_cols]),
    "impossible_power": (F.least(P, Q, I, S1, S2, S3) < 0) | (P > rules.MAX_ACTIVE_KW),
    "voltage_out_of_range": (V < rules.VOLTAGE_MIN) | (V > rules.VOLTAGE_MAX),
    "submeters_exceed_total": S1 + S2 + S3 > P * 1000 / 60 + rules.SUBMETER_ROUNDING_WH,
    "duplicate_timestamp": F.row_number().over(first_of_minute) > 1,
}
assert list(failed) == list(rules.BATCH_CHECKS)

typed = (typed.withColumn("_line", F.monotonically_increasing_id())
         .withColumn("reject_reason", F.coalesce(*[
             F.when(F.coalesce(check, F.lit(False)), F.lit(name)) for name, check in failed.items()]))
         .drop("_line")
         .cache())

clean = (typed.filter(F.col("reject_reason").isNull())
         .select("ts", *measure_cols)
         .withColumn("year", F.year("ts")))
rejected = typed.filter(F.col("reject_reason").isNotNull())

# The duplicate check shuffles rows by minute, so put them back in time order before writing:
# one file per year, sorted by ts, which is what makes the Parquet copy compress well. (The
# writer sorts by its partition column, year, first; sorting by year then ts keeps our order.)
clean.repartition("year").sortWithinPartitions("year", "ts").write.mode("overwrite").partitionBy("year").parquet(OUT)
rejected.coalesce(1).sortWithinPartitions("ts").write.mode("overwrite").parquet(QUARANTINE)

# Count what actually landed on disk, not what we intended to write.
rows_in = typed.count()
rows_out = spark.read.parquet(OUT).count()
rows_rejected = spark.read.parquet(QUARANTINE).count()
elapsed = time.time() - start

print("\n=== Ingest summary ===")
print(f"rows in (raw file):      {rows_in}")
print(f"rows out (Parquet):      {rows_out}")
print(f"rows quarantined:        {rows_rejected}")
print(f"in == out + quarantined: {rows_in == rows_out + rows_rejected}")

# Rows failed per check, counted from what actually landed in quarantine.
by_reason = dict(spark.read.parquet(QUARANTINE).groupBy("reject_reason").count().collect())
checks = {name: by_reason.get(name, 0) for name in rules.BATCH_CHECKS}
print(f"\n=== Quality checks (rows set aside under the first check they fail; alert above "
      f"{rules.ALERT_THRESHOLD:.0%}) ===")
print(f"{'check':<28}{'rows failed':>12}{'% of rows':>11}  what it catches")
for name, rows_failed in checks.items():
    print(f"{name:<28}{rows_failed:>12,}{100 * rows_failed / rows_in:>10.3f}%  {rules.BATCH_CHECKS[name]}")
raised = rules.alerts(checks, rows_in)
for name, rows_failed, rate in raised:
    print(f"ALERT: {name} failed {rows_failed:,} of {rows_in:,} rows ({rate:.2%}), "
          f"above the {rules.ALERT_THRESHOLD:.0%} threshold")
if not raised:
    print(f"No alert: every check is below the {rules.ALERT_THRESHOLD:.0%} threshold")

os.makedirs(os.path.dirname(QUALITY_REPORT), exist_ok=True)
with open(QUALITY_REPORT, "w") as f:
    json.dump({"stage": "ingest", "rows_in": rows_in, "rows_out": rows_out,
               "rows_set_aside": rows_rejected, "checks": checks}, f, indent=2)

print("\n=== Size before and after ===")
raw_size, pq_size = data_bytes(RAW), data_bytes(OUT)
print(f"raw text file: {mb(raw_size)}")
print(f"Parquet:       {mb(pq_size)}")
print(f"ratio:         {raw_size / pq_size:.1f}x smaller")

print("\n=== Parquet schema ===")
spark.read.parquet(OUT).printSchema()
print(f"Ingest took {elapsed:.1f} s")

spark.stop()
