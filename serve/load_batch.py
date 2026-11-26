"""Part 3a: load the Spark batch results and the ingest quarantine into PostgreSQL.

It also records, for Part 4, rows in and out for the ingest, batch-query and load stages, the
rows failed per ingest check, and an alert for any check above the threshold (quality/rules.py).

Each table is emptied and reloaded in one transaction, so running this again is safe and a
failed load leaves the previous contents in place. Rows in and rows out are counted for every
table, and the job fails if any table lost or gained rows.

Run with the rest of the system (docker compose up), or on its own:
  docker compose run --rm load-batch
"""
import json
import sys
import time

import pyarrow.dataset as ds
from psycopg import sql

from quality import rules
from serve.db import connect, save_batch_stage

# Parquet folder -> (table, {parquet column: table column} for any column that is renamed)
SOURCES = {
    "data/results/q1_share": ("q1_share", {}),
    "data/results/q1_seasonal": ("q1_seasonal", {}),
    "data/results/q1_monthly": ("q1_monthly", {}),
    "data/results/q2_hourly": ("q2_hourly", {}),
    "data/results/q3_hourly": ("q3_hourly", {}),
    "data/results/q3_runs": ("q3_runs", {}),
    "data/quarantine/ingest": ("quarantine_ingest", {"Date": "raw_date", "Time": "raw_time"}),
}
QUALITY_REPORTS = {"1 ingest": "data/quality/ingest.json", "2 batch queries": "data/quality/batch_queries.json"}


def load(conn, path, table, renames):
    """Replace the contents of `table` with the Parquet data at `path`. Returns (rows in, rows out)."""
    data = ds.dataset(path, format="parquet").to_table()
    columns = [renames.get(name, name) for name in data.column_names]
    copy_sql = sql.SQL("COPY {} ({}) FROM STDIN").format(
        sql.Identifier(table), sql.SQL(", ").join(map(sql.Identifier, columns)))

    with conn.transaction(), conn.cursor() as cur:
        cur.execute(sql.SQL("TRUNCATE {}").format(sql.Identifier(table)))
        with cur.copy(copy_sql) as copy:
            for batch in data.to_batches():
                for row in zip(*(column.to_pylist() for column in batch.columns)):
                    copy.write_row(row)
        rows_out = cur.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table))).fetchone()[0]
    return data.num_rows, rows_out


def main():
    start = time.time()
    failed = 0
    print("\n=== Part 3: loading batch results into PostgreSQL ===")
    print(f"{'table':<20}{'rows in (Parquet)':>20}{'rows out (Postgres)':>22}  match")
    total_in = total_out = 0
    with connect() as conn:
        for path, (table, renames) in SOURCES.items():
            rows_in, rows_out = load(conn, path, table, renames)
            ok = rows_in == rows_out
            failed += not ok
            total_in, total_out = total_in + rows_in, total_out + rows_out
            print(f"{table:<20}{rows_in:>20,}{rows_out:>22,}  {ok}")
        print(f"Loaded {len(SOURCES)} tables in {time.time() - start:.1f} s, {failed} with a row-count mismatch")

        # Part 4: counts and check results for the batch stages, kept next to the stream's.
        for order, (stage, path) in enumerate(QUALITY_REPORTS.items(), 1):
            with open(path) as f:
                report = json.load(f)
            check_order = rules.BATCH_CHECKS if "checks" in report else ()
            raised = rules.alerts(report["checks"], report["rows_in"]) if "checks" in report else []
            save_batch_stage(conn, stage, order, report, check_order, raised)
            for name, rows_failed, rate in raised:
                print(f"ALERT ({stage}): {name} failed {rows_failed:,} rows ({rate:.2%})")
        save_batch_stage(conn, "3 load batch", 3,
                         {"rows_in": total_in, "rows_out": total_out, "rows_set_aside": 0})
        print("Recorded stage counts and check results for ingest, batch queries and this load")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
