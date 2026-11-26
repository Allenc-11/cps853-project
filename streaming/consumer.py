"""
Part 2b: Consumer

- 'Consumes' events from the power-usage topic.
- Maintains running totals
- Prints updated results every 20 seconds.
- Will answer question 2: Which hours of the day use the most power, and which sub-meter drives it?

Topic: power-usage
Interval: 20 seconds
"""

import json
import time
from datetime import datetime
from collections import Counter

from kafka import KafkaConsumer

TOPIC = "power-usage"
BROKER = "kafka:9092" # 9092 is typical for apache kafka. kafka as runs inside docker
GROUP_ID = "power-usage-consumer" # group id for consumers. There is only one
UPDATE_INTERVAL = 20

TS_FORMAT = "%d/%m/%Y %H:%M:%S"

# raw column name -> clean column name
MEASURES = {
    "Global_active_power": "global_active_power",      # kW (minute average)
    "Global_reactive_power": "global_reactive_power",  # kW (minute average)
    "Voltage": "voltage",                              # V
    "Global_intensity": "global_intensity",            # A
    "Sub_metering_1": "sub_metering_1",                # Wh in the minute (kitchen)
    "Sub_metering_2": "sub_metering_2",                # Wh in the minute (laundry)
    "Sub_metering_3": "sub_metering_3",                # Wh in the minute (water heater + A/C)
}

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BROKER,
    group_id=GROUP_ID,
    auto_offset_reset="earliest", # Reads earliest undread message possible
    enable_auto_commit=True, # Sets offsets periodically
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

seen_keys = set()

last_timestamp = None

def problems(key, row, seen_keys):
    global last_timestamp
    issues = []
    
    # Duplicate check
    if key in seen_keys:
        issues.append("duplicate")
    else:
        seen_keys.add(key)

    # Lateness Check
    timestamp = datetime.strptime(row["ts"], TS_FORMAT) # %d/%m/%Y %H:%M:%S
    if last_timestamp is not None and timestamp <= last_timestamp:
        issues.append("late")
    else:
        last_timestamp = timestamp

    # Bad Data
    measurements = [
        row.get(name) for name in MEASURES
    ]

    if all(value is None for value in measurements):
        issues.append("all_measurements_missing")

    return issues

clean = []
quarantine = []

last_update = time.monotonic()

# Which hours of the day use the most power, and which sub-meter drives it?
for message in consumer:

    if message.key == b"END":
        print("Producer end message received. Stopping")
        break

    row = message.value
    key = message.key

    issues = problems(key, row, seen_keys)

    if not issues:
        clean.append(row)
    else:
        quarantine.append({
            "key" : key.decode("utf-8", errors="replace"),
            "row" : row,
            "reasons" : issues
        })

    # Only every 20 seconds
    now = time.monotonic()
    if now - last_update >= UPDATE_INTERVAL:        
        print(f"Clean rows: {len(clean)}:,", flush=True)
        print(f"Bad rows: {len(quarantine):,} : ", flush=True)
        last_update = now

consumer.close()

print("\n=== Consumer summary ===")
print(f"Clean rows: {len(clean)}:,")
print(f"Bad rows: {len(quarantine):,} : ")
reason_counts = Counter(
    reason
    for row in quarantine
    for reason in row["reasons"]
)
for reason, count in reason_counts.items():
    print(f"{reason}: {count:,}")