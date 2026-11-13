"""Part 1a: convert the raw semicolon-separated text file to Parquet.

- Parses Date + Time into one timestamp column `ts` (stored as wall-clock time, session TZ = UTC).
- Casts the 7 measurement columns to double.
- Rows that cannot be used are set aside in data/quarantine/ingest (not dropped), with a reason.
- Prints rows in / rows out / rows rejected and the size of the data before and after.

Run from the project root (Windows PowerShell):
  docker run --rm --user root -v "${PWD}:/work" -w /work spark:python3 /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g jobs/ingest.py
"""
import os
import time

from pyspark.sql import SparkSession, functions as F

RAW = "data/raw/household_power_consumption.txt"
OUT = "data/parquet/power"
QUARANTINE = "data/quarantine/ingest"
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
typed = typed.withColumn(
    "reject_reason",
    F.when(F.col("ts").isNull(), F.lit("unparseable_timestamp"))
     .when(F.coalesce(*[F.col(c) for c in measure_cols]).isNull(), F.lit("all_measurements_missing")),
).cache()

clean = (typed.filter(F.col("reject_reason").isNull())
         .select("ts", *measure_cols)
         .withColumn("year", F.year("ts")))
rejected = typed.filter(F.col("reject_reason").isNotNull())

clean.write.mode("overwrite").partitionBy("year").parquet(OUT)
rejected.write.mode("overwrite").parquet(QUARANTINE)

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
print("\n=== Quarantined rows by reason ===")
spark.read.parquet(QUARANTINE).groupBy("reject_reason").count().show(truncate=False)

print("=== Rows with SOME (not all) measurements missing, kept in Parquet ===")
partial = spark.read.parquet(OUT).filter(
    F.greatest(*[F.col(c).isNull().cast("int") for c in measure_cols]) == 1).count()
print(partial)

print("\n=== Size before and after ===")
raw_size, pq_size = data_bytes(RAW), data_bytes(OUT)
print(f"raw text file: {mb(raw_size)}")
print(f"Parquet:       {mb(pq_size)}")
print(f"ratio:         {raw_size / pq_size:.1f}x smaller")

print("\n=== Parquet schema ===")
spark.read.parquet(OUT).printSchema()
print(f"Ingest took {elapsed:.1f} s")

spark.stop()
