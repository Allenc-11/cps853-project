
"""
Part 2b: Consumer

- 'Consumes' events from the power-usage topic, which the producer fills with every line of the
  raw file, in the file's order (event-time order), bad readings included.
- Keeps running totals for every event-time hour, and saves them to PostgreSQL (table
  stream_hourly) every 5 seconds, so Part 3 can answer questions while rows are still arriving.
- Prints the running answer to question 2 every 20 seconds:
  Which hours of the day use the most power, and which sub-meter drives it?
- Part 4: checks every message with the same rules as the batch job (quality/rules.py), so the
  stream sets aside the same raw readings as ingest.py. A message
  that fails is set aside in table stream_rejects with the check it failed; a reading whose event
  time is not after the newest one applied (late or repeated) is counted, not added. Failures
  per check are saved in quality_checks, and an alert is raised in table alerts for every window
  of 10,000 messages in which more than 2% fail (in a normal run, the windows where the meter
  recorded nothing for a long stretch). Corrupted copies the producer adds for the Part 4
  experiment carry an "injected" header: their failures are also counted on their own, so the
  experiment can compare them with what was injected. The checks themselves ignore the header.
- Restarts where it stopped: totals, counts and the Kafka offset are saved in the same
  transaction (serve/db.py), and on start the consumer reloads them and reads on from that offset.

Topic: power-usage
Print interval: 20 seconds. Save interval: 5 seconds.
Run from the project root:  python -m streaming.consumer
Setting: QUALITY_CHECKS=off skips the range and late checks, to measure what they prevent (Part 4).
"""

import json
import math
import os
import time
from collections import Counter
from datetime import datetime

from kafka import KafkaConsumer, TopicPartition

from quality import rules
from serve import db

TOPIC = "power-usage"
PARTITION = 0  # producer.py writes every row to partition 0, which keeps them in event-time order
BROKER = "kafka:9092" # 9092 is typical for apache kafka. kafka as runs inside docker
PRINT_INTERVAL = 20
SAVE_INTERVAL = 5
TS_FORMAT = "%d/%m/%Y %H:%M:%S"  # as in the raw file, which producer.py sends unchanged. Ex: 16/12/2006 17:24:00
CHECKS_ON = os.environ.get("QUALITY_CHECKS", "on") != "off"


def validate(raw, checks_on=True):
    """Return ((ts, [7 measures]), None) for a usable reading, or (None, check failed).

    The measures are in rules.MEASURES order. Checks run in rules.STREAM_CHECKS order; the late
    check needs the consumer's state, so main() applies it. Without checks_on, only what is needed
    to add the reading up at all is checked, as the consumer did before Part 4.
    """
    try:
        row = json.loads(raw)
    except (TypeError, ValueError):  # not text, not UTF-8, or not JSON
        return None, "unreadable_message"
    if not isinstance(row, dict):
        return None, "unreadable_message"
    try:
        ts = datetime.strptime(row["ts"], TS_FORMAT)
    except (KeyError, TypeError, ValueError):
        return None, "unparseable_timestamp"
    values = []
    for name in rules.MEASURES:
        value = row.get(name)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            return None, "missing_measurement"
        values.append(float(value))
    failed = rules.range_check(*values) if checks_on else None
    return ((ts, values), None) if failed is None else (None, failed)


# Which hours of the day use the most power, and which sub-meter drives it?
def display_results(hours, progress, elapsed, update_number):
    by_hour = {}  # hour of day -> [minutes, active kW, meter 1, meter 2, meter 3]
    for hour_start, t in hours.items():
        acc = by_hour.setdefault(hour_start.hour, [0, 0.0, 0.0, 0.0, 0.0])
        acc[0] += t.minutes
        acc[1] += t.active_kw_sum
        acc[2] += t.sm1_wh
        acc[3] += t.sm2_wh
        acc[4] += t.sm3_wh

    print("=== Q2: average active power by hour of day (highest first) ===", flush=True)
    print(f"Time since consuming: {elapsed:.0f} seconds. Update number {update_number}. "
          f"Readings so far: {progress.rows_applied:,}, up to {progress.last_ts}. "
          f"Set aside: {progress.rows_rejected:,}, late: {progress.rows_late:,}", flush=True)

    sorted_hours = sorted(by_hour.items(), key=lambda item: item[1][1] / item[1][0], reverse=True)  # Descending order
    for hour, (count, active, meter1, meter2, meter3) in sorted_hours:
        print(f"Hour {hour:2d} | Active Power {active / count:.3f} kW | Sub-Meter 1 {meter1 / count:.3f} | "
              f"Sub-Meter 2 {meter2 / count:.3f} | Sub-Meter 3 {meter3 / count:.3f} (Wh per minute)", flush=True)
    print("\n", flush=True)


def main():
    conn = db.connect()
    hours, progress = db.load_stream_state(conn, TOPIC, PARTITION)  # {hour_start: HourTotals}
    run_start_counts = (progress.messages_in, progress.rows_applied, progress.rows_rejected, progress.rows_late)
    print(f"Quality checks: {'on' if CHECKS_ON else 'OFF (range and late checks skipped)'}", flush=True)

    consumer = KafkaConsumer(
        bootstrap_servers=BROKER,
        enable_auto_commit=False,  # the offset is saved in PostgreSQL with the totals instead
        auto_offset_reset="earliest",  # if the saved offset no longer exists (topic recreated), start over
    )
    partition = TopicPartition(TOPIC, PARTITION)
    consumer.assign([partition])
    if progress.next_offset is None:
        print("Starting from the beginning of the topic", flush=True)
        consumer.seek_to_beginning(partition)
    else:
        print(f"Resuming at offset {progress.next_offset:,} with {progress.rows_applied:,} readings "
              f"already saved, up to {progress.last_ts}", flush=True)
        consumer.seek(partition, progress.next_offset)

    changed = {}  # hours changed since the last save
    rejects = []  # (kafka offset, check failed, raw value) not saved yet
    new_alerts = []  # (check, rows checked, rows failed, detail) not saved yet
    window, window_start = Counter(), None  # failures per check in the current window of messages
    window_size = 0
    producer = None  # the producer's own counts, from its END message

    start_time = last_save = last_print = time.monotonic()

    def save():
        if progress.next_offset is None:  # nothing read yet
            return
        db.save_stream_state(conn, TOPIC, PARTITION, changed, progress, rejects, rules.STREAM_CHECKS,
                             new_alerts, producer, time.monotonic() - start_time)
        changed.clear()
        rejects.clear()
        new_alerts.clear()

    def fail(check, injected):
        progress.check_failures[check] = progress.check_failures.get(check, 0) + 1
        if injected:
            progress.injected_failures[check] = progress.injected_failures.get(check, 0) + 1
        window[check] += 1

    update_number = 0
    finished = False

    while not finished:
        records = consumer.poll(timeout_ms=1000, max_records=5000).get(partition, [])
        for message in records:
            progress.next_offset = message.offset + 1

            if message.key == b"END":
                print("Producer end message received. Stopping", flush=True)
                try:
                    producer = json.loads(message.value)
                except (TypeError, ValueError):
                    producer = None
                finished = True
                break

            progress.messages_in += 1
            window_start = message.offset if window_start is None else window_start
            window_size += 1

            injected = any(name == "injected" for name, _ in message.headers or ())  # Part 4 experiment only
            reading, check = validate(message.value, CHECKS_ON)
            if reading is None:
                progress.rows_rejected += 1
                fail(check, injected)
                raw = message.value.decode("utf-8", errors="replace")[:1000] if message.value is not None else None
                rejects.append((message.offset, check, raw))
            else:
                ts, (active, reactive, _voltage, _intensity, sm1, sm2, sm3) = reading
                if CHECKS_ON and progress.last_ts is not None and ts <= progress.last_ts:
                    progress.rows_late += 1
                    fail("late_or_duplicate", injected)
                else:
                    hour_start = ts.replace(minute=0, second=0)
                    totals = hours.get(hour_start)
                    if totals is None:
                        totals = hours[hour_start] = db.HourTotals()
                    totals.add(active, reactive, sm1, sm2, sm3)
                    changed[hour_start] = totals
                    progress.last_ts = max(ts, progress.last_ts) if progress.last_ts else ts
                    progress.rows_applied += 1

            # Alert when too many messages in a window fail (quality/rules.py).
            if window_size == rules.STREAM_ALERT_WINDOW:
                detail = f"messages at offsets {window_start:,} to {message.offset:,}"
                for name, rows_failed, rate in rules.alerts(window, window_size):
                    new_alerts.append((name, window_size, rows_failed, detail))
                    print(f"ALERT: {name} failed {rows_failed:,} of {window_size:,} messages ({rate:.2%}) "
                          f"in {detail}", flush=True)
                window, window_start, window_size = Counter(), None, 0

        now = time.monotonic()
        if finished or now - last_save >= SAVE_INTERVAL:
            save()
            last_save = now
        if hours and (finished or now - last_print >= PRINT_INTERVAL):
            update_number += 1
            last_print = now
            display_results(hours, progress, now - start_time, update_number)

    consumer.close()
    conn.close()

    run_counts = [total - before for total, before in zip(
        (progress.messages_in, progress.rows_applied, progress.rows_rejected, progress.rows_late), run_start_counts)]
    print("\n=== Consumer summary ===")
    print(f"This run:  consumed {run_counts[0]:,} messages: {run_counts[1]:,} applied, "
          f"{run_counts[2]:,} set aside, {run_counts[3]:,} late or repeated.")
    print(f"All runs:  consumed {progress.messages_in:,} messages: {progress.rows_applied:,} applied, "
          f"{progress.rows_rejected:,} set aside, {progress.rows_late:,} late or repeated.")
    print(f"in == applied + set aside + late: "
          f"{progress.messages_in == progress.rows_applied + progress.rows_rejected + progress.rows_late}")
    if producer:
        delivered = producer.get("messages_delivered", producer["messages_sent"])
        print(f"Producer sent {producer['messages_sent']:,} messages, Kafka accepted {delivered:,}; "
              f"consumed {progress.messages_in:,} ({progress.messages_in - delivered:+,}: more means messages "
              f"added by hand, fewer means messages lost)")
    print("Failures by check: " + ", ".join(
        f"{name} {progress.check_failures.get(name, 0):,}" for name in rules.STREAM_CHECKS))
    if progress.injected_failures:
        print("Of which corrupted copies added on purpose: " + ", ".join(
            f"{name} {progress.injected_failures.get(name, 0):,}" for name in rules.STREAM_CHECKS))
    print(f"Hours saved to PostgreSQL: {len(hours):,}")
    print(f"Time taken: {time.monotonic() - start_time:.2f} seconds")
    print(f"Amount of updates: {update_number}")


if __name__ == "__main__":
    main()
