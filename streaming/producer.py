
"""
Part 2a: Producer

- Reads the raw text file (data/raw), the same file ingest.py (Part 1a) reads, so the stream gets
  every reading as the meter recorded it, bad ones included. The quality checks happen in the
  consumer (Part 4), not here.
- Replays the lines in file order, which is event-time order (checked on the full file: every
  timestamp is later than the one before, one line per minute).
- Part 2, delivery problems a real stream has (David's design): with DUPLICATE_EVERY = n, every
  n-th reading is sent twice; with LATE_EVERY = n, every n-th reading is held back and sent
  LATE_BY readings later, out of event-time order. The consumer must add a late reading to its
  own hour and count a duplicate only once. Both are off unless set (docker-compose.yml sets them).
- Sends each line as a JSON message, changing only its form, not its content: the timestamp as
  written ("16/12/2006 17:24:00"), and each measurement as a number, or null where the file has
  "?" or anything else that is not a number (the same as ingest.py's nullValue "?" and try_cast).
  A line without the expected 9 fields is sent unchanged, as text, so the consumer sets it aside.
- Publishes at an accelerated speed and counts Kafka's answer for every message: delivered, or
  failed (for example a delivery that timed out because the broker was too slow). A failed
  message is lost, so it is counted, not hidden.
- Ends with an END message that carries its own counts (lines read, messages sent, delivered and
  failed), so the consumer can check that every delivered message arrived (Part 4).
- Part 4 experiment: with CORRUPT_RATE above 0, adds a corrupted copy after that share of the
  valid readings (quality/inject.py), marked with an "injected" Kafka header so the experiment
  can tell its catches from the real bad readings. The checks never look at the header, and the
  real readings are always sent unchanged.

Topic: power-usage
Message format: JSON
Replay speed: SPEED messages per second (default 10000)
Run from the project root:  python -m streaming.producer
Settings (environment variables): SPEED, CORRUPT_RATE (0 to 1, default 0), CORRUPT_SEED (default 853),
DUPLICATE_EVERY (default 0 = off), LATE_EVERY (default 0 = off), LATE_BY (default 1000)
"""

# https://kafka.apache.org/community/developer/
# Output result constantly updated as more data added (consumer)

import json
import os
import random
import time
from collections import Counter, deque

from kafka import KafkaProducer

from quality.inject import can_corrupt, corrupt
from quality.rules import MEASURES

RAW = "data/raw/household_power_consumption.txt"
FIELDS = 2 + len(MEASURES)  # Date;Time;7 measurements
TOPIC = "power-usage"
BROKER = "kafka:9092" # 9092 is typical for apache kafka. kafka as runs inside docker
SPEED = int(os.environ.get("SPEED", "10000"))
CORRUPT_RATE = float(os.environ.get("CORRUPT_RATE", "0"))
CORRUPT_SEED = int(os.environ.get("CORRUPT_SEED", "853"))  # same seed, same corrupted rows
DUPLICATE_EVERY = int(os.environ.get("DUPLICATE_EVERY", "0"))  # every n-th reading sent twice; 0 = off
LATE_EVERY = int(os.environ.get("LATE_EVERY", "0"))  # every n-th reading delivered late; 0 = off
LATE_BY = int(os.environ.get("LATE_BY", "1000"))  # how many readings later a late one is sent


def number_or_none(text):
    """The raw value as a number, or None for "?" or anything else that is not a number."""
    try:
        return float(text)
    except ValueError:
        return None


def to_message(line):
    """(key, value) for one line of the raw file: JSON for a line with the expected fields,
    otherwise the line itself, unchanged."""
    fields = line.rstrip("\r\n").split(";")
    if len(fields) != FIELDS:
        return None, None, line.rstrip("\r\n").encode("utf-8")
    row = {"ts": f"{fields[0]} {fields[1]}"}
    row.update(zip(MEASURES, map(number_or_none, fields[2:])))
    return row, row["ts"].encode("utf-8"), json.dumps(row).encode("utf-8")


producer = KafkaProducer(
    bootstrap_servers=BROKER,
    acks="all" # acknowledgement wait
)

delivery = Counter()  # Kafka's answer for each message: "delivered" or "failed"


def on_delivered(_metadata):
    delivery["delivered"] += 1


def on_failed(_error):
    delivery["failed"] += 1


def send(key, value, headers=None):
    (producer.send(TOPIC, key=key, value=value, headers=headers, partition=0)
     .add_callback(on_delivered).add_errback(on_failed))


chooser = random.Random(CORRUPT_SEED)
injected = Counter()
duplicates_sent = 0
delayed = 0
lines_read = 0
sent = 0
next_pace_check = 1000
start = time.monotonic() # Cannot go backwards


def emit(n, row, key, value):
    """Send the n-th reading of the file, plus a duplicate and a corrupted copy when they are due."""
    global sent, duplicates_sent, next_pace_check

    # Sending data from producer
    send(key, value)
    sent += 1

    # Part 2: the same reading delivered twice
    if DUPLICATE_EVERY and n % DUPLICATE_EVERY == 0:
        send(key, value)
        duplicates_sent += 1
        sent += 1

    # Part 4 experiment: a corrupted copy straight after a valid reading
    if CORRUPT_RATE and row is not None and can_corrupt(row) and chooser.random() < CORRUPT_RATE:
        kind, bad_value = corrupt(row, sum(injected.values()))
        send(key, bad_value, headers=[("injected", kind.encode("utf-8"))])
        injected[kind] += 1
        sent += 1

    # Speed control. --> sent/SPEED is expected time ; time.monotonic() - start is actual
    if sent >= next_pace_check:
        next_pace_check += 1000
        delay = sent / SPEED - (time.monotonic() - start)
        if delay > 0:
            time.sleep(delay) # Ensures streaming cannot occur faster than expected


held_back = deque()  # (send after this line number, n, row, key, value): late readings, oldest first

with open(RAW, encoding="utf-8") as raw:
    next(raw)  # header line
    for lines_read, line in enumerate(raw, 1):
        message = to_message(line)

        # Part 2: hold this reading back and send it LATE_BY readings later
        if LATE_EVERY and lines_read % LATE_EVERY == 0:
            held_back.append((lines_read + LATE_BY, lines_read, *message))
            delayed += 1
        else:
            emit(lines_read, *message)

        while held_back and held_back[0][0] <= lines_read:
            emit(*held_back.popleft()[1:])

        # Message every 50000 lines
        if lines_read % 50000 == 0:
            print(f"Sent {lines_read:,} events", flush=True) # {i:,} -> formats numbers with ',' separator. Flush to force message out

while held_back:  # late readings still waiting when the file ends
    emit(*held_back.popleft()[1:])


producer.flush()  # wait for Kafka's answer on every message, so the END message has final counts
end = {"end": True, "rows_read": lines_read, "messages_sent": sent,
       "messages_delivered": delivery["delivered"], "messages_failed": delivery["failed"],
       "duplicates_sent": duplicates_sent, "delayed": delayed,
       "injected_total": sum(injected.values()), "injected": dict(injected)}
producer.send(TOPIC, key=b"END", value=json.dumps(end).encode("utf-8"), partition=0).get(timeout=60)  # b for bytes


producer.close()

print("\n=== Producer summary ===")
print(f"Total: read {lines_read:,} lines, sent {sent:,} messages: "
      f"{delivery['delivered']:,} delivered, {delivery['failed']:,} failed (lost).")
if duplicates_sent or delayed:
    print(f"Delivery problems on purpose: {duplicates_sent:,} readings sent twice, "
          f"{delayed:,} sent {LATE_BY:,} readings late.")
if injected:
    print(f"Corrupted copies added on purpose (rate {CORRUPT_RATE:.1%}): {dict(injected)}")

time_taken = time.monotonic() - start
print(f"Time taken: {time_taken:.2f} seconds")
print(f"Throughput: {sent / time_taken:,.0f} msg/s")
