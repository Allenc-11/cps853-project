"""Part 4 experiment: measure one stream run against the clean batch results.

Run after a stream run has finished (quality/run_experiment.sh does this). Prints, and appends to
data/quality/experiment.csv:
- what was injected and what the checks caught, kind by kind (only the corrupted copies, marked
  by the producer's "injected" header, not the real bad readings in the raw file);
- how many alerts were raised, out of how many windows (including the windows where the real
  data is missing for a long stretch, which raise alerts in a normal run too);
- how far the stream's answers are from the batch answers (built from clean Parquet), for Q1
  (total energy), Q2 (average power by hour of day) and Q3 (hours with power factor below the
  threshold in questions.py);
- the consumer's throughput.

  docker compose run --rm --no-deps -e RUN_LABEL="checks on, 5%" quality-measure
"""
import csv
import os

from questions import PF_THRESHOLD
from quality import rules
from serve.db import connect
from serve.queries import show

EXPERIMENT_CSV = "data/quality/experiment.csv"

CAUGHT = """
    SELECT i.kind, i.expected_check, i.messages AS injected, coalesce(c.rows_failed_injected, 0) AS caught
    FROM injected_messages i LEFT JOIN quality_checks c
         ON c.stage = 'stream' AND c.check_name = i.expected_check
    ORDER BY i.kind"""

# Stream vs batch, both per clock hour. Batch q3_hourly is built from the clean Parquet data.
ACCURACY = f"""
    WITH s AS (SELECT hour_start, minutes, active_kw_sum AS p, reactive_kw_sum AS q FROM stream_hourly),
         b AS (SELECT hour_start, minutes, p, q FROM q3_hourly),
         hod AS (SELECT extract(hour FROM b.hour_start) AS h,
                        sum(b.p) / sum(b.minutes) AS batch_kw,
                        sum(coalesce(s.p, 0)) / nullif(sum(coalesce(s.minutes, 0)), 0) AS stream_kw
                 FROM b LEFT JOIN s USING (hour_start) GROUP BY 1)
    SELECT (SELECT sum(minutes) FROM b) AS batch_readings,
           (SELECT sum(minutes) FROM s) AS stream_readings,
           (SELECT count(*) FROM b FULL JOIN s USING (hour_start)
             WHERE b.minutes IS DISTINCT FROM s.minutes
                OR abs(b.p - s.p) > 1e-9 * greatest(1, abs(b.p))) AS hours_that_differ,
           100 * ((SELECT sum(p) FROM s) / (SELECT sum(p) FROM b) - 1) AS q1_energy_error_pct,
           (SELECT max(abs(stream_kw - batch_kw)) FROM hod) AS q2_max_error_kw,
           (SELECT count(*) FROM s WHERE p / nullif(sqrt(p * p + q * q), 0) < {PF_THRESHOLD})
             - (SELECT count(*) FROM b WHERE p / nullif(sqrt(p * p + q * q), 0) < {PF_THRESHOLD}) AS q3_low_pf_hours_error"""

STREAM = """
    SELECT (SELECT rows_in FROM pipeline_counts WHERE stage = '5 consumer') AS messages,
           (SELECT seconds FROM pipeline_counts WHERE stage = '5 consumer') AS seconds,
           (SELECT coalesce(sum(messages), 0) FROM injected_messages) AS injected,
           (SELECT coalesce(sum(rows_failed_injected), 0) FROM quality_checks WHERE stage = 'stream') AS caught,
           (SELECT count(*) FROM alerts WHERE stage = 'stream') AS alerts"""


def main():
    label = os.environ.get("RUN_LABEL", "run")
    with connect() as conn:
        print(f"\n=== Part 4 experiment: {label} ===")
        cur = conn.execute(CAUGHT)
        caught_rows = cur.fetchall()
        if caught_rows:
            show([d.name for d in cur.description], caught_rows)
        messages, seconds, injected, caught, alerts = conn.execute(STREAM).fetchone()
        accuracy = conn.execute(ACCURACY).fetchone()

    batch_readings, stream_readings, hours_differ, q1_err, q2_err, q3_err = accuracy
    windows = messages // rules.STREAM_ALERT_WINDOW
    injected = injected or 0
    result = {
        "run": label,
        "messages": messages,
        "injected": injected,
        "caught": caught,
        "injected_kinds_fully_caught": f"{sum(1 for r in caught_rows if r[3] == r[2])}/{len(caught_rows)}",
        "alerts": alerts,
        "windows": windows,
        "stream_readings": stream_readings,
        "batch_readings": batch_readings,
        "hours_that_differ": hours_differ,
        "q1_energy_error_pct": round(q1_err, 4),
        "q2_max_error_kw": round(q2_err, 4),
        "q3_low_pf_hours_error": q3_err,
        "consumer_msgs_per_s": round(messages / seconds) if seconds else None,
    }
    show(list(result), [list(result.values())])

    new_file = not os.path.exists(EXPERIMENT_CSV)
    with open(EXPERIMENT_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(result))
        if new_file:
            writer.writeheader()
        writer.writerow(result)
    print(f"Appended to {EXPERIMENT_CSV}")


if __name__ == "__main__":
    main()
