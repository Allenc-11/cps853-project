"""Part 1b: answer the team's questions from the Parquet data with Spark.

Q1 Where is active power used, and how does it change by month and season?
Q2 Which hours of the day use the most power, and which sub-meter drives it?
Q3 How efficient is the household's electrical power usage, based on its power
   factor (the relationship between active and reactive power)? Are there periods of
   at least 2 consecutive hours where the power factor falls below 0.95, potentially
   indicating inefficient electrical equipment or abnormal operating conditions?

Results are printed and also saved as Parquet in data/results/ so Part 3 can load them.

Run from the project root (Windows PowerShell):
  docker run --rm --user root -v "${PWD}:/work" -w /work spark:python3 /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g jobs/batch_queries.py
"""
import time

from pyspark.sql import SparkSession, Window, functions as F

PARQUET = "data/parquet/power"
RESULTS = "data/results"
MINUTES_PER_DAY = 1440

# Q3 parameters (team decision, 2026-11-13)
PF_THRESHOLD = 0.95  # an hour is "low power factor" when its power factor is below this
MIN_RUN_HOURS = 2    # a period must last at least this many consecutive hours

spark = (SparkSession.builder.appName("batch_queries")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# Energy per minute in Wh. global_active_power is a kW average over the minute,
# so kW * 1000 / 60 = Wh. "other" is energy not covered by the three sub-meters
# (formula from the UCI dataset description).
df = (spark.read.parquet(PARQUET)
      .withColumn("total_wh", F.col("global_active_power") * 1000 / 60)
      .withColumn("other_wh", F.col("total_wh") - F.col("sub_metering_1")
                  - F.col("sub_metering_2") - F.col("sub_metering_3")))

ENERGY = {
    "kitchen_sm1": "sub_metering_1",
    "laundry_sm2": "sub_metering_2",
    "heater_ac_sm3": "sub_metering_3",
    "other": "other_wh",
    "total": "total_wh",
}


def timed(label, fn):
    t = time.time()
    result = fn()
    print(f"[{label}] {time.time() - t:.2f} s")
    return result


# ---------- Q1: by month and by season ----------
def q1():
    season = (F.when(F.month("ts").isin(12, 1, 2), "winter")
               .when(F.month("ts").isin(3, 4, 5), "spring")
               .when(F.month("ts").isin(6, 7, 8), "summer")
               .otherwise("fall"))
    # Sums are divided by days observed so months with missing minutes or partial
    # coverage (Dec 2006, Nov 2010) are comparable: result is kWh per day.
    per_day = [(F.sum(col) / 1000 / (F.count("*") / MINUTES_PER_DAY)).alias(f"{name}_kwh_per_day")
               for name, col in ENERGY.items()]

    monthly = (df.groupBy(F.date_format("ts", "yyyy-MM").alias("month"))
               .agg(F.count("*").alias("minutes_observed"), *per_day)
               .orderBy("month"))
    seasonal = (df.groupBy(season.alias("season"))
                .agg(F.count("*").alias("minutes_observed"), *per_day)
                .orderBy(F.desc("total_kwh_per_day")))
    totals = df.agg(*[(F.sum(col) / 1000).alias(name) for name, col in ENERGY.items()]).first()
    share = spark.createDataFrame(
        [(name, round(totals[name], 1), round(100 * totals[name] / totals["total"], 1))
         for name in ENERGY if name != "total"],
        ["category", "kwh", "pct_of_total"]).orderBy(F.desc("kwh"))

    print("\n=== Q1a: share of active energy by category (whole period) ===")
    share.show(truncate=False)
    print("=== Q1b: kWh per day by season ===")
    seasonal.show(truncate=False)
    print("=== Q1c: kWh per day by month ===")
    monthly.show(60, truncate=False)

    share.write.mode("overwrite").parquet(f"{RESULTS}/q1_share")
    seasonal.write.mode("overwrite").parquet(f"{RESULTS}/q1_seasonal")
    monthly.write.mode("overwrite").parquet(f"{RESULTS}/q1_monthly")


# ---------- Q2: by hour of day ----------
def q2():
    hourly = (df.groupBy(F.hour("ts").alias("hour"))
              .agg(F.avg("global_active_power").alias("avg_active_kw"),
                   *[F.avg(col).alias(f"{name}_avg_wh_per_min")
                     for name, col in ENERGY.items() if name != "total"])
              .orderBy(F.desc("avg_active_kw")))
    print("\n=== Q2: average active power by hour of day (highest first) ===")
    hourly.show(24, truncate=False)
    hourly.write.mode("overwrite").parquet(f"{RESULTS}/q2_hourly")


# ---------- Q3: power factor ----------
def q3():
    # Hourly power factor = P / sqrt(P^2 + Q^2), from the hour's summed active (P)
    # and reactive (Q) power. 1.0 = all power does useful work; lower = more reactive power.
    by_hour = (df.groupBy(F.date_trunc("hour", "ts").alias("hour_start"))
               .agg(F.sum("global_active_power").alias("p"),
                    F.sum("global_reactive_power").alias("q"),
                    F.avg("global_active_power").alias("avg_active_kw"),
                    F.count("*").alias("minutes"))
               .withColumn("power_factor",
                           F.try_divide(F.col("p"), F.sqrt(F.col("p") ** 2 + F.col("q") ** 2)))
               .withColumn("low_pf", F.col("power_factor") < PF_THRESHOLD))

    overall = by_hour.agg(
        F.count("*").alias("hours"),
        F.avg("power_factor").alias("avg_power_factor"),
        F.min("power_factor").alias("min_power_factor"),
        F.sum(F.col("low_pf").cast("int")).alias("hours_below_threshold"),
        F.avg(F.col("low_pf").cast("int")).alias("share_hours_below_threshold"))
    print(f"\n=== Q3a: power factor overview (threshold = {PF_THRESHOLD}) ===")
    overall.show(truncate=False)

    # Consecutive low-PF hours ("gaps and islands"): within a run, hour_index minus
    # row_number is constant. An hour with no data breaks a run.
    flagged = (by_hour.filter(F.col("low_pf"))
               .withColumn("hour_index", (F.unix_timestamp("hour_start") / 3600).cast("long")))
    w = Window.orderBy("hour_index")  # single partition; fine for ~34k hourly rows
    runs = (flagged.withColumn("grp", F.col("hour_index") - F.row_number().over(w))
            .groupBy("grp")
            .agg(F.min("hour_start").alias("run_start"),
                 F.max("hour_start").alias("run_last_hour"),
                 F.count("*").alias("hours"),
                 F.avg("power_factor").alias("avg_power_factor"),
                 F.min("power_factor").alias("min_power_factor"),
                 # Shown so we can tell real faults from low-use periods (e.g. house empty)
                 F.avg("avg_active_kw").alias("avg_active_kw"))
            .filter(F.col("hours") >= MIN_RUN_HOURS)
            .drop("grp")
            .orderBy(F.desc("hours")))
    print(f"=== Q3b: periods of >= {MIN_RUN_HOURS} consecutive hours with power factor < {PF_THRESHOLD} (longest first) ===")
    print(f"number of periods: {runs.count()}")
    runs.show(20, truncate=False)

    by_hour.write.mode("overwrite").parquet(f"{RESULTS}/q3_hourly")
    runs.write.mode("overwrite").parquet(f"{RESULTS}/q3_runs")


df.cache()
print(f"rows read from Parquet: {timed('load + count', df.count)}")
timed("Q1", q1)
timed("Q2", q2)
timed("Q3", q3)
spark.stop()
