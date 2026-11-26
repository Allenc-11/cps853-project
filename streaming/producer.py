
"""
Part 2a: Producer

- Reads parquest data product from ingest.py (Part 1a)
- Sorts rows by timestamp.
- Publishes JSON messages to Kafka in order at an accelerated speed.
- Counts Kafka's answer for every message: delivered, or failed (for example a delivery that
  timed out because the broker was too slow). A failed message is lost, so it is counted, not hidden.
- Ends with an END message that carries its own counts (rows read, messages sent, delivered and
  failed), so the consumer can check that every delivered message arrived (Part 4).
- Part 4 experiment: with CORRUPT_RATE above 0, adds a corrupted copy after that share of the
  readings (quality/inject.py). The real readings are always sent unchanged.

Topic: power-usage
Message format: JSON
Replay speed: SPEED messages per second (default 10000)
Run from the project root:  python -m streaming.producer
Settings (environment variables): SPEED, CORRUPT_RATE (0 to 1, default 0), CORRUPT_SEED (default 853)
"""

# https://kafka.apache.org/community/developer/
# Will run on clean data (ingest.py must run first)
# Output result constantly updated as more data added (consumer)

import json
import os
import random
import time
from collections import Counter

import pyarrow.dataset as ds
from kafka import KafkaProducer

from quality.inject import corrupt

PARQUET = "data/parquet/power"
TOPIC = "power-usage"
BROKER = "kafka:9092" # 9092 is typical for apache kafka. kafka as runs inside docker
SPEED = int(os.environ.get("SPEED", "10000"))
CORRUPT_RATE = float(os.environ.get("CORRUPT_RATE", "0"))
CORRUPT_SEED = int(os.environ.get("CORRUPT_SEED", "853"))  # same seed, same corrupted rows

# Retrieveing the data from parquest file as dataframe
table = ds.dataset(PARQUET, format="parquet").to_table() # All columns

# Sort by event time
rows = table.sort_by([("ts", "ascending")]).to_pylist()

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    acks="all" # acknowledgement wait
)

"""Data is now loaded. Now actually streaming data"""

delivery = Counter()  # Kafka's answer for each message: "delivered" or "failed"


def on_delivered(_metadata):
    delivery["delivered"] += 1


def on_failed(_error):
    delivery["failed"] += 1


def send(key, value):
    producer.send(TOPIC, key=key, value=value, partition=0).add_callback(on_delivered).add_errback(on_failed)


chooser = random.Random(CORRUPT_SEED)
injected = Counter()
sent = 0
next_pace_check = 1000
start = time.monotonic() # Cannot go backwards

for i, row in enumerate(rows, 1):
    row["ts"] = row["ts"].strftime("%Y-%m-%d %H:%M:%S") # Converts datetime to string
    key = row["ts"].encode("utf-8")

    # Sending data from producer
    send(key, json.dumps(row).encode("utf-8"))
    sent += 1

    # Part 4 experiment: a corrupted copy straight after the real reading
    if CORRUPT_RATE and chooser.random() < CORRUPT_RATE:
        kind, value = corrupt(row, sum(injected.values()))
        send(key, value)
        injected[kind] += 1
        sent += 1

    # Message every 50000 events
    if i % 50000 == 0:
        print(f"Sent {i:,} events", flush=True) # {i:,} -> formats numbers with ',' separator. Flush to force message out

    # Speed control. --> sent/SPEED is expected time ; time.monotonic() - start is actual
    if sent >= next_pace_check:
        next_pace_check += 1000
        delay = sent / SPEED - (time.monotonic() - start)
        if delay > 0:
            time.sleep(delay) # Ensures streaming cannot occur faster than expected


producer.flush()  # wait for Kafka's answer on every message, so the END message has final counts
end = {"end": True, "rows_read": len(rows), "messages_sent": sent,
       "messages_delivered": delivery["delivered"], "messages_failed": delivery["failed"],
       "injected_total": sum(injected.values()), "injected": dict(injected)}
producer.send(TOPIC, key=b"END", value=json.dumps(end).encode("utf-8"), partition=0).get(timeout=60)  # b for bytes


producer.close()

print("\n=== Producer summary ===")
print(f"Total: read {len(rows):,} readings, sent {sent:,} messages: "
      f"{delivery['delivered']:,} delivered, {delivery['failed']:,} failed (lost).")
if injected:
    print(f"Corrupted copies added on purpose (rate {CORRUPT_RATE:.1%}): {dict(injected)}")

time_taken = time.monotonic() - start
print(f"Time taken: {time_taken:.2f} seconds")
print(f"Throughput: {sent / time_taken:,.0f} msg/s")
