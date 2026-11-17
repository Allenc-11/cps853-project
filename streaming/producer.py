
"""
Part 2a: Producer

- Reads parquest data product from ingest.py (Part 1a)
- Sorts rows by timestamp.
- Publishes JSON messages to Kafka in order at an accelerated speed.

Topic: power-usage
Message format: JSON
Replay speed: 10000/s
"""

# https://kafka.apache.org/community/developer/
# Will run on clean data (ingest.py must run first)
# Output result constantly updated as more data added (consumer)

import json
import time

import pyarrow.dataset as ds
from kafka import KafkaProducer

PARQUET = "data/parquet/power"
TOPIC = "power-usage"
BROKER = "kafka:9092" # 9092 is typical for apache kafka. kafka as runs inside docker
SPEED = 10_000

# Retrieveing the data from parquest file as dataframe
table = ds.dataset(PARQUET, format="parquet").to_table() # All columns

# Sort by event time
rows = table.sort_by([("ts", "ascending")]).to_pylist()

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all" # acknowledgement wait
)

"""Data is now loaded. Now actually streaming data"""

start = time.monotonic() # Cannot go backwards

for i, row in enumerate(rows, 1):
    row["ts"] = row["ts"].strftime("%Y-%m-%d %H:%M:%S") # Converts datetime to string

    # Sending data from producer
    producer.send(
        TOPIC,
        key=row["ts"].encode("utf-8"),
        value=row,
        partition=0
    )

    # Message every 50000 events
    if i % 50000 == 0:
        print(f"Sent {i:,} events", flush=True) # {i:,} -> formats numbers with ',' separator. Flush to force message out

    # Speed control. --> i/SPEED is expected time ; time.monotonic() - start is actual
    if i % 1000 == 0:
        delay = i / SPEED - (time.monotonic() - start)
        if delay > 0:
            time.sleep(delay) # Ensures streaming cannot occur faster than expected


producer.send(TOPIC, key= b"END", value="END", partition=0) # b for bytes
producer.flush()


producer.close()

print("\n=== Producer summary ===")
print(f"Total: sent {len(rows):,} readings.")

time_taken = time.monotonic() - start
print(f"Time taken: {time_taken:.2f} seconds")
print(f"Throughput: {len(rows) / time_taken:,.0f} msg/s")
