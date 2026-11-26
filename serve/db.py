"""Parts 3 and 4: shared PostgreSQL access for the batch loader, the stream consumer, the
queries and the quality report.

Connection settings come from the standard libpq environment variables (PGHOST, PGPORT,
PGDATABASE, PGUSER, PGPASSWORD), which docker-compose.yml sets for each service.
The tables are created by serve/schema.sql when the database is first started.
"""
from dataclasses import dataclass, field
from datetime import datetime

import psycopg

from quality.inject import KINDS
from quality.rules import ALERT_THRESHOLD


def connect():
    """Autocommit connection: every write below is wrapped in an explicit transaction."""
    return psycopg.connect(autocommit=True, options="-c TimeZone=UTC")


# ---------- Stream state: running totals per event-time hour, plus how far the consumer got ----------

@dataclass(slots=True)
class HourTotals:
    """Running totals for one clock hour. Averages are worked out at query time (sum / minutes)."""
    minutes: int = 0
    active_kw_sum: float = 0.0
    reactive_kw_sum: float = 0.0
    sm1_wh: float = 0.0
    sm2_wh: float = 0.0
    sm3_wh: float = 0.0

    def add(self, active_kw, reactive_kw, sm1_wh, sm2_wh, sm3_wh):
        self.minutes += 1
        self.active_kw_sum += active_kw
        self.reactive_kw_sum += reactive_kw
        self.sm1_wh += sm1_wh
        self.sm2_wh += sm2_wh
        self.sm3_wh += sm3_wh


@dataclass(slots=True)
class StreamProgress:
    """Where the consumer is in the topic, and what it has done with every message so far."""
    next_offset: int | None = None  # None: nothing saved yet, so start at the beginning of the topic
    last_ts: datetime | None = None  # newest event time applied; anything at or before it is late
    messages_in: int = 0
    rows_applied: int = 0
    rows_rejected: int = 0
    rows_late: int = 0
    check_failures: dict = field(default_factory=dict)  # {stream check name: messages failed}
    injected_failures: dict = field(default_factory=dict)  # the same, for corrupted copies only (Part 4 experiment)


_HOUR_COLUMNS = "minutes, active_kw_sum, reactive_kw_sum, sm1_wh, sm2_wh, sm3_wh"

_UPSERT_HOUR = f"""
    INSERT INTO stream_hourly (hour_start, {_HOUR_COLUMNS})
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (hour_start) DO UPDATE SET
        minutes = EXCLUDED.minutes,
        active_kw_sum = EXCLUDED.active_kw_sum,
        reactive_kw_sum = EXCLUDED.reactive_kw_sum,
        sm1_wh = EXCLUDED.sm1_wh,
        sm2_wh = EXCLUDED.sm2_wh,
        sm3_wh = EXCLUDED.sm3_wh,
        updated_at = now()
"""

_UPSERT_PROGRESS = """
    INSERT INTO stream_progress (topic, kafka_partition, next_offset, last_ts,
                                 messages_in, rows_applied, rows_rejected, rows_late)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (topic, kafka_partition) DO UPDATE SET
        next_offset = EXCLUDED.next_offset,
        last_ts = EXCLUDED.last_ts,
        messages_in = EXCLUDED.messages_in,
        rows_applied = EXCLUDED.rows_applied,
        rows_rejected = EXCLUDED.rows_rejected,
        rows_late = EXCLUDED.rows_late,
        updated_at = now()
"""


def load_stream_state(conn, topic, partition):
    """Return (hours, progress) as last saved, so a restarted consumer carries on where it stopped."""
    hours = {
        hour_start: HourTotals(*totals)
        for hour_start, *totals in conn.execute(f"SELECT hour_start, {_HOUR_COLUMNS} FROM stream_hourly")
    }
    row = conn.execute(
        """SELECT next_offset, last_ts, messages_in, rows_applied, rows_rejected, rows_late
           FROM stream_progress WHERE topic = %s AND kafka_partition = %s""",
        (topic, partition)).fetchone()
    progress = StreamProgress(*row) if row else StreamProgress()
    for name, failed, failed_injected in conn.execute(
            "SELECT check_name, rows_failed, rows_failed_injected FROM quality_checks WHERE stage = 'stream'"):
        progress.check_failures[name] = failed
        progress.injected_failures[name] = failed_injected
    return hours, progress


def save_stream_state(conn, topic, partition, changed_hours, progress, rejects,
                      check_order, new_alerts=(), producer=None, seconds=None):
    """Save changed hours, progress, check counts, stage counts, rejected messages and alerts in
    one transaction.

    Hours and counts are written as absolute totals, not increments, and the Kafka offset is
    saved in the same transaction. A crash therefore loses nothing that was saved and repeats
    nothing: the restarted consumer reloads this state and reads again from progress.next_offset.

    changed_hours: {hour_start: HourTotals}; rejects: [(kafka_offset, reason, raw_value)];
    check_order: stream check names in order; new_alerts: [(check, rows_checked, rows_failed, detail)];
    producer: the producer's own counts from its END message, once received.
    """
    with conn.transaction(), conn.cursor() as cur:
        if changed_hours:
            cur.executemany(_UPSERT_HOUR, [
                (hour_start, t.minutes, t.active_kw_sum, t.reactive_kw_sum, t.sm1_wh, t.sm2_wh, t.sm3_wh)
                for hour_start, t in changed_hours.items()])
        if rejects:
            cur.executemany(
                "INSERT INTO stream_rejects (kafka_offset, reason, raw_value) VALUES (%s, %s, %s)", rejects)
        cur.execute(_UPSERT_PROGRESS, (
            topic, partition, progress.next_offset, progress.last_ts, progress.messages_in,
            progress.rows_applied, progress.rows_rejected, progress.rows_late))
        cur.executemany(_UPSERT_CHECK, [
            ("stream", order, name, progress.messages_in, progress.check_failures.get(name, 0),
             progress.injected_failures.get(name, 0))
            for order, name in enumerate(check_order, 1)])
        cur.executemany(_INSERT_ALERT, [
            ("stream", name, checked, failed, failed / checked, ALERT_THRESHOLD, detail)
            for name, checked, failed, detail in new_alerts])
        if producer:
            cur.execute("DELETE FROM injected_messages")
            cur.executemany(
                "INSERT INTO injected_messages (kind, expected_check, messages) VALUES (%s, %s, %s)",
                [(kind, KINDS[kind], n) for kind, n in producer.get("injected", {}).items()])
            # Out = messages Kafka accepted, so a failed (lost) delivery makes this stage not balance.
            failed = producer.get("messages_failed", 0)
            notes = [f"{producer['injected_total']:,} corrupted messages added on purpose"] if producer["injected_total"] else []
            if failed:
                notes.append(f"{failed:,} messages failed to reach Kafka (lost)")
            cur.execute(_UPSERT_STAGE, (
                "4 producer", 4, producer["rows_read"], producer["injected_total"],
                producer.get("messages_delivered", producer["messages_sent"]), 0, None,
                "; ".join(notes) or None))
        cur.execute(_UPSERT_STAGE, (
            "5 consumer", 5, progress.messages_in, 0, progress.rows_applied,
            progress.rows_rejected + progress.rows_late, seconds,
            f"{progress.rows_rejected:,} set aside in stream_rejects, {progress.rows_late:,} late or repeated"))


# ---------- Part 4: stage counts, check results and alerts ----------

_UPSERT_STAGE = """
    INSERT INTO pipeline_counts (stage, stage_order, rows_in, rows_added, rows_out, rows_set_aside, seconds, note)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (stage) DO UPDATE SET
        stage_order = EXCLUDED.stage_order, rows_in = EXCLUDED.rows_in, rows_added = EXCLUDED.rows_added,
        rows_out = EXCLUDED.rows_out, rows_set_aside = EXCLUDED.rows_set_aside,
        seconds = EXCLUDED.seconds, note = EXCLUDED.note, updated_at = now()
"""

_UPSERT_CHECK = """
    INSERT INTO quality_checks (stage, check_order, check_name, rows_checked, rows_failed, rows_failed_injected)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (stage, check_name) DO UPDATE SET
        check_order = EXCLUDED.check_order, rows_checked = EXCLUDED.rows_checked,
        rows_failed = EXCLUDED.rows_failed, rows_failed_injected = EXCLUDED.rows_failed_injected,
        updated_at = now()
"""

_INSERT_ALERT = """
    INSERT INTO alerts (stage, check_name, rows_checked, rows_failed, fail_rate, threshold, detail)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""


def save_batch_stage(conn, stage, stage_order, report, check_order=(), raised=()):
    """Record one batch stage's counts, its check results, and its alerts (replacing earlier ones).

    report: {"rows_in", "rows_out", "rows_set_aside", optional "checks": {name: rows failed}};
    raised: [(check, rows failed, rate)] from quality.rules.alerts().
    """
    checks = report.get("checks", {})
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(_UPSERT_STAGE, (stage, stage_order, report["rows_in"], 0, report["rows_out"],
                                    report["rows_set_aside"], None, None))
        cur.executemany(_UPSERT_CHECK, [(stage, order, name, report["rows_in"], checks.get(name, 0), 0)
                                        for order, name in enumerate(check_order, 1)])
        cur.execute("DELETE FROM alerts WHERE stage = %s", (stage,))
        cur.executemany(_INSERT_ALERT, [
            (stage, name, report["rows_in"], failed, rate, ALERT_THRESHOLD, None) for name, failed, rate in raised])
