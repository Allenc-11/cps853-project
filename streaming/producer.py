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
# Will run on raw data (creates parquet file with raw data)
# Output result constantly updated as more data added (consumer)

import json
import time
from collections import deque

import pyarrow.dataset as ds
import pandas as pd
from kafka import KafkaProducer


CSV = "data/raw/household_power_consumption.txt"
PARQUET = "data/raw/parquet"
TOPIC = "power-usage"
BROKER = "kafka:9092" # 9092 is typical for apache kafka. kafka as runs inside docker

SPEED = 10_000 # Number of messages 'produced' per second
DUPLICATE = 157 # All messages with id % 157 == 0 are duplicated
LATE = 254 # All messages with id % 254 == 0 are delayed
LATE_DELAY = 1000 # 1000 message delay (wait for 1000 messages)

# Converting RAW DATA (CSV) into Parquet file
df = pd.read_csv(CSV, sep=";", na_values=["?"])
df["ts"] = pd.to_datetime(df["Date"] + " " + df["Time"], format="%d/%m/%Y %H:%M:%S")
df = df.drop(columns=["Date", "Time"])
df.to_parquet(PARQUET, engine="pyarrow", index=False)

# Retrieveing the data from parquest file as dataframe
table = ds.dataset(PARQUET, format="parquet").to_table() # All columns

# Sort by event time
rows = table.sort_by([("ts", "ascending")]).to_pylist()

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v, allow_nan=False).encode("utf-8"),
    acks="all" # acknowledgement wait
)

"""Data is now loaded. Now actually streaming data"""

late_messages = deque() # Uses queue to quickly pop left

count = 0
pace_check = 1000 # Check needed as count can increase by 2
start = time.monotonic() # Cannot go backwards

def send_message(row, i):
    global count, pace_check, start

    # Sending data from producer
    producer.send(
        TOPIC,
        key=row["ts"].encode("utf-8"), # Key is timestamp
        value=row, # Value is the whole row
        partition=0
    )
    count += 1

    # Sending duplicated message
    if i % DUPLICATE == 0:
        producer.send(
            TOPIC,
            key=row["ts"].encode("utf-8"),
            value=row,
            partition=0
        )
        count += 1
    
    # Message every 50000 events sent through producer
    if count % 50000 == 0:
        print(f"Sent {count:,} events", flush=True) # {i:,} -> formats numbers with ',' separator. Flush to force message out

    # Speed control. --> i/SPEED is expected time ; time.monotonic() - start is actual
    if count >= pace_check:
        pace_check += 1000
        delay = count / SPEED - (time.monotonic() - start)
        if delay > 0:
            time.sleep(delay) # Ensures streaming cannot occur faster than expected


for i, row in enumerate(rows, 1):
    
    # Convert NaN to None (JSON null)
    row = {
        k: None if pd.isna(v) else v
        for k, v in row.items()
    }

    row["ts"] = row["ts"].strftime("%d/%m/%Y %H:%M:%S") # Converts datetime to string

    # Late message
    if i % LATE == 0:
        late_messages.append((i, row))
    else:
        send_message(row, i)

    while late_messages:
        if i - late_messages[0][0] < LATE_DELAY: # Checks that the difference between current and late message is at least late_delay
            break

        late_i, late_row = late_messages.popleft() # Getting rid of first element

        # Sending late data from producer
        send_message(late_row, late_i)

# Send remaining late messages if any
while late_messages:
        late_i, late_row = late_messages.popleft() # Getting rid of first element

        # Sending late data from producer
        send_message(late_row, late_i)

producer.send(TOPIC, key= b"END", value="END", partition=0) # b for bytes
producer.flush()


producer.close()

print("\n=== Producer summary ===")
print(f"Total: read {len(rows):,} readings, sent {count:,} messages, "
      f"{count - len(rows):,} duplicates.")

time_taken = time.monotonic() - start
print(f"Time taken: {time_taken:.2f} seconds")
print(f"Throughput: {count / time_taken:,.0f} msg/s")