"""Part 4: one report of the whole pipeline's quality, read from PostgreSQL.

1. Rows in and rows out at every stage, whether each stage balances, whether every row a
   stage passes on reaches the next one, and whether the batch and stream paths, which both
   start from the raw file, read the same rows and keep the same rows.
2. Rows failed per check, batch and stream, against the alert threshold.
3. Alerts raised.
4. Where the rows that were set aside are, and a sample of the latest ones.

Run with the rest of the system (docker compose up), or at any time, even mid-stream:
  docker compose run --rm --no-deps quality-report
"""
from quality import rules
from serve.db import connect
from serve.queries import show

STAGES = """
    SELECT stage, rows_in, rows_added, rows_out, rows_set_aside,
           rows_in + rows_added = rows_out + rows_set_aside AS balances, note
    FROM pipeline_counts ORDER BY stage_order"""

CHECKS = """
    SELECT stage, check_name AS check, rows_checked, rows_failed,
           round(100.0 * rows_failed / nullif(rows_checked, 0), 3) AS pct_failed,
           CASE WHEN rows_failed > %(threshold)s * rows_checked THEN 'ALERT' ELSE 'ok' END AS status
    FROM quality_checks ORDER BY stage = 'stream', check_order"""

ALERT_SUMMARY = """
    SELECT stage, check_name AS check, count(*) AS times_raised,
           round(100 * max(fail_rate)::numeric, 2) AS worst_pct, max(raised_at) AS latest
    FROM alerts GROUP BY stage, check_name ORDER BY stage, check_name"""

LATEST_ALERTS = """
    SELECT stage, check_name AS check, rows_failed, rows_checked,
           round(100 * fail_rate::numeric, 2) AS pct, detail
    FROM alerts ORDER BY id DESC LIMIT 3"""

SET_ASIDE = """
    SELECT 'quarantine_ingest' AS table_name, reject_reason AS reason, count(*) AS rows
    FROM quarantine_ingest GROUP BY reject_reason
    UNION ALL
    SELECT 'stream_rejects', reason, count(*) FROM stream_rejects GROUP BY reason
    ORDER BY 1, 3 DESC"""

# Rows a stage passes on must all arrive at the next one: (from stage, to stage).
HANDOFFS = (("1 ingest", "2 batch queries"), ("4 producer", "5 consumer"))

# The batch path (1 ingest) and the stream path (4 producer, 5 consumer) both read the raw file.
# (label, stage, column, stage, column): the two numbers should be equal.
PATHS_AGREE = (
    ("raw lines read", "1 ingest", "rows_in", "4 producer", "rows_in"),
    ("rows kept after the checks", "1 ingest", "rows_out", "5 consumer", "rows_out"),
)
COLUMNS = {"rows_in": 1, "rows_out": 3}  # positions in a STAGES row

LATEST_REJECTS = """
    SELECT kafka_offset, reason, left(raw_value, 70) AS raw_value
    FROM stream_rejects ORDER BY id DESC LIMIT 5"""


def section(conn, title, sql, params=None, empty="  none"):
    print(f"\n=== {title} ===")
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    if rows:
        show([d.name for d in cur.description], rows)
    else:
        print(empty)
    return rows


def print_handoffs(stages):
    """Rows out of one stage against rows into the next: fewer means rows were lost between them."""
    print("\n=== Rows passed between stages (out of one = into the next) ===")
    for source, target in HANDOFFS:
        if source not in stages or target not in stages:
            continue
        sent, received = stages[source][3], stages[target][1]
        gap = received - sent
        status = ("ok" if gap == 0 else f"LOST {-gap:,} rows" if gap < 0
                  else f"{gap:,} more rows (added by hand, not by {source})")
        print(f"  {source} -> {target}: {sent:,} out, {received:,} in: {status}")


def print_paths_agree(stages):
    """Batch and stream start from the same raw file and apply the same checks."""
    print("\n=== Batch and stream paths, both from the raw file ===")
    for label, batch, batch_col, stream, stream_col in PATHS_AGREE:
        if batch not in stages or stream not in stages:
            continue
        b, s = stages[batch][COLUMNS[batch_col]], stages[stream][COLUMNS[stream_col]]
        status = "equal" if b == s else f"DIFFER by {s - b:+,} (stream - batch)"
        print(f"  {label}: batch ({batch}) {b:,}, stream ({stream}) {s:,}: {status}")


def main():
    with connect() as conn:
        print("\n=== Part 4: data quality, from PostgreSQL ===")
        stages = section(conn, "Rows in and out at every stage (in + added = out + set aside)", STAGES)
        unbalanced = [s[0] for s in stages if not s[5]]
        print(f"  stages that do not balance: {', '.join(unbalanced) if unbalanced else 'none'}")
        print_handoffs({s[0]: s for s in stages})
        print_paths_agree({s[0]: s for s in stages})

        section(conn, f"Quality checks (a row counts under the first check it fails; "
                      f"alert above {rules.ALERT_THRESHOLD:.0%})",
                CHECKS, {"threshold": rules.ALERT_THRESHOLD})

        section(conn, f"Alerts raised (batch: whole run; stream: per {rules.STREAM_ALERT_WINDOW:,} messages)",
                ALERT_SUMMARY, empty=f"  none: no check failed on more than {rules.ALERT_THRESHOLD:.0%} of rows")
        section(conn, "Latest alerts", LATEST_ALERTS)

        section(conn, "Rows set aside, by table and reason", SET_ASIDE)
        section(conn, "Latest stream messages set aside", LATEST_REJECTS)


if __name__ == "__main__":
    main()
