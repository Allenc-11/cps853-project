"""Part 3b: answer the team's three questions from PostgreSQL, and time every answer.

Q1 Where is most of the power used, and how does it change by month and season?
Q2 Which hours of the day use the most power, and why?
Q3 How efficient is the household's power use (power factor)? Are there periods of 2+ hours
   where the power factor is below 0.95?

Each query is answered from the batch tables (Spark results) or the stream tables (written live
by the consumer). Response time is measured from this client: 1 warm-up run, then RUNS timed
runs, reported as median and 95th percentile. The database's own time comes from
EXPLAIN ANALYZE (median of EXPLAIN_RUNS). Timings are also written to data/results/serve_timings.csv for the report.

Run with the rest of the system (docker compose up), or at any time, even mid-stream:
  docker compose run --rm --no-deps serve-queries
"""
import csv
import math
import statistics
import time

from serve.db import connect

RUNS = 30
EXPLAIN_RUNS = 5
TIMINGS_CSV = "data/results/serve_timings.csv"
MONTHLY_YEAR = "2009"  # Q1c shows one full year month by month

# Name the largest of the four uses, from per-minute Wh averages or sums of the same unit.
MAIN_USE = """CASE greatest({other}, {heater}, {laundry}, {kitchen})
                  WHEN {other} THEN 'unmetered (other)'
                  WHEN {heater} THEN 'water heater + A/C'
                  WHEN {laundry} THEN 'laundry'
                  ELSE 'kitchen' END"""

QUERIES = [
    ("Q1a", "Where is the energy used? Share by category, whole period", "batch", """
        SELECT category, round(kwh::numeric) AS kwh, pct_of_total
        FROM q1_share
        ORDER BY kwh DESC"""),

    ("Q1b", "How does use change by season? kWh per day", "batch", """
        SELECT season,
               round(total_kwh_per_day::numeric, 1)         AS total,
               round(other_kwh_per_day::numeric, 1)         AS other,
               round(heater_ac_sm3_kwh_per_day::numeric, 1) AS heater_ac,
               round(laundry_sm2_kwh_per_day::numeric, 1)   AS laundry,
               round(kitchen_sm1_kwh_per_day::numeric, 1)   AS kitchen
        FROM q1_seasonal
        ORDER BY total_kwh_per_day DESC"""),

    ("Q1c", f"How does use change month to month? kWh per day in {MONTHLY_YEAR}", "batch", f"""
        SELECT month,
               round(total_kwh_per_day::numeric, 1)         AS total,
               round(other_kwh_per_day::numeric, 1)         AS other,
               round(heater_ac_sm3_kwh_per_day::numeric, 1) AS heater_ac,
               round((minutes_observed / 1440.0)::numeric, 1) AS days_observed
        FROM q1_monthly
        WHERE month BETWEEN '{MONTHLY_YEAR}-01' AND '{MONTHLY_YEAR}-12'
        ORDER BY month"""),

    ("Q2", "Which hours of the day use the most power, and why? Top 5", "batch", f"""
        SELECT hour,
               round(avg_active_kw::numeric, 2)                AS avg_kw,
               round(other_avg_wh_per_min::numeric, 1)         AS other_wh,
               round(heater_ac_sm3_avg_wh_per_min::numeric, 1) AS heater_ac_wh,
               round(laundry_sm2_avg_wh_per_min::numeric, 1)   AS laundry_wh,
               round(kitchen_sm1_avg_wh_per_min::numeric, 1)   AS kitchen_wh,
               {MAIN_USE.format(other="other_avg_wh_per_min", heater="heater_ac_sm3_avg_wh_per_min",
                                laundry="laundry_sm2_avg_wh_per_min", kitchen="kitchen_sm1_avg_wh_per_min")}
                   AS main_use
        FROM q2_hourly
        ORDER BY avg_active_kw DESC
        LIMIT 5"""),

    ("Q2-live", "Which hours use the most power, from the stream so far? Top 5", "stream", f"""
        SELECT hour, round((active / minutes)::numeric, 2) AS avg_kw, minutes AS readings_so_far,
               {MAIN_USE.format(other="other", heater="sm3", laundry="sm2", kitchen="sm1")} AS main_use
        FROM (SELECT extract(hour FROM hour_start)::int AS hour,
                     sum(minutes) AS minutes, sum(active_kw_sum) AS active,
                     sum(sm1_wh) AS sm1, sum(sm2_wh) AS sm2, sum(sm3_wh) AS sm3,
                     sum(active_kw_sum) * 1000 / 60 - sum(sm1_wh) - sum(sm2_wh) - sum(sm3_wh) AS other
              FROM stream_hourly
              GROUP BY 1) AS by_hour
        ORDER BY active / minutes DESC
        LIMIT 5"""),

    ("Q3a", "How efficient is the household? Power factor per hour, overview", "batch", """
        SELECT count(*)                                       AS hours,
               round(avg(power_factor)::numeric, 3)           AS avg_pf,
               round(min(power_factor)::numeric, 3)           AS lowest_pf,
               count(*) FILTER (WHERE power_factor < 0.95)    AS hours_below_095,
               round(100.0 * count(*) FILTER (WHERE power_factor < 0.95) / count(power_factor), 1)
                                                              AS pct_below_095,
               (SELECT count(*) FROM q3_runs)                 AS runs_of_2h_or_more
        FROM q3_hourly"""),

    ("Q3b", "Longest periods of 2+ hours with power factor below 0.95. Top 5", "batch", """
        SELECT run_start, run_last_hour, hours,
               round(avg_power_factor::numeric, 3) AS avg_pf,
               round(min_power_factor::numeric, 3) AS lowest_pf,
               round(avg_active_kw::numeric, 2)    AS avg_kw
        FROM q3_runs
        ORDER BY hours DESC, run_start
        LIMIT 5"""),

    ("Q3-live", "Is the power factor low right now? Latest 6 hours from the stream", "stream", """
        SELECT hour_start, minutes,
               round((active_kw_sum / nullif(sqrt(active_kw_sum ^ 2 + reactive_kw_sum ^ 2), 0))::numeric, 3)
                   AS power_factor,
               CASE WHEN active_kw_sum / nullif(sqrt(active_kw_sum ^ 2 + reactive_kw_sum ^ 2), 0) < 0.95
                    THEN 'below 0.95' ELSE '' END AS flag
        FROM stream_hourly
        ORDER BY hour_start DESC
        LIMIT 6"""),
]

# Does the stream, once finished, agree with the batch job hour by hour? Both are built from the
# same 2,049,270 Parquet rows, so every hour should match (floating-point sums to 1e-9 relative).
RECONCILE = """
    SELECT (SELECT sum(minutes) FROM q3_hourly)     AS batch_readings,
           (SELECT sum(minutes) FROM stream_hourly) AS stream_readings,
           count(*)                                 AS hours_compared,
           count(*) FILTER (WHERE b.hour_start IS NULL OR s.hour_start IS NULL
                               OR b.minutes <> s.minutes
                               OR abs(b.p - s.active_kw_sum) > 1e-9 * greatest(1, abs(b.p))
                               OR abs(b.q - s.reactive_kw_sum) > 1e-9 * greatest(1, abs(b.q)))
                                                    AS hours_that_differ
    FROM q3_hourly b FULL JOIN stream_hourly s USING (hour_start)"""

STREAM_PROGRESS = """
    SELECT messages_in, rows_applied, rows_rejected, rows_late, last_ts AS newest_reading,
           (SELECT count(*) FROM stream_hourly) AS hours_in_db
    FROM stream_progress"""


def show(columns, rows):
    """Print rows as an aligned text table."""
    cells = [[("" if v is None else str(v)) for v in row] for row in rows]
    widths = [max([len(c)] + [len(r[i]) for r in cells]) for i, c in enumerate(columns)]
    print("  " + " | ".join(c.ljust(w) for c, w in zip(columns, widths)))
    print("  " + "-+-".join("-" * w for w in widths))
    for r in cells:
        print("  " + " | ".join(v.ljust(w) for v, w in zip(r, widths)))


def run(conn, sql):
    cur = conn.execute(sql)
    return [d.name for d in cur.description], cur.fetchall()


def time_query(conn, sql):
    """Client round-trip times in ms (median, p95) over RUNS runs, and the server's own time."""
    run(conn, sql)  # warm-up: plan cache and data pages
    samples = []
    for _ in range(RUNS):
        start = time.perf_counter()
        run(conn, sql)
        samples.append((time.perf_counter() - start) * 1000)
    samples.sort()
    p95 = samples[math.ceil(0.95 * RUNS) - 1]
    server_ms = statistics.median(server_time(conn, sql) for _ in range(EXPLAIN_RUNS))
    return statistics.median(samples), p95, server_ms


def server_time(conn, sql):
    """Planning + execution time reported by the database itself. EXPLAIN ANALYZE adds a little
    timing overhead of its own, so for tiny queries this can exceed the client round trip."""
    plan = conn.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + sql).fetchone()[0][0]
    return plan["Planning Time"] + plan["Execution Time"]


def main():
    timings = []
    with connect() as conn:
        progress = run(conn, STREAM_PROGRESS)
        print("\n=== Part 3: answers from PostgreSQL ===")
        print("\nStream so far (written by the consumer):")
        if progress[1]:
            show(*progress)
        else:
            print("  nothing yet: the consumer has not saved any readings")

        for key, question, source, sql in QUERIES:
            columns, rows = run(conn, sql)
            median_ms, p95_ms, server_ms = time_query(conn, sql)
            timings.append((key, question, source, len(rows), median_ms, p95_ms, server_ms))
            print(f"\n=== {key}: {question} [{source} tables] ===")
            show(columns, rows)
            print(f"  response time: median {median_ms:.2f} ms, p95 {p95_ms:.2f} ms over {RUNS} runs "
                  f"(database alone: {server_ms:.2f} ms)")

        print("\n=== Check: stream and batch agree, hour by hour ===")
        show(*run(conn, RECONCILE))

    print("\n=== Response times (ms) ===")
    show(["query", "source", "rows", "median", "p95", "database alone"],
         [(k, s, n, f"{m:.2f}", f"{p:.2f}", f"{d:.2f}") for k, _, s, n, m, p, d in timings])

    with open(TIMINGS_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["query", "question", "source", "rows", "median_ms", "p95_ms", "server_ms", "runs"])
        for k, q, s, n, m, p, d in timings:
            writer.writerow([k, q, s, n, f"{m:.3f}", f"{p:.3f}", f"{d:.3f}", RUNS])
    print(f"\nSaved timings to {TIMINGS_CSV}")


if __name__ == "__main__":
    main()
