
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

from kafka import KafkaConsumer

TOPIC = "power-usage"
BROKER = "kafka:9092" # 9092 is typical for apache kafka. kafka as runs inside docker
GROUP_ID = "power-usage-consumer" # group id for consumers. There is only one
UPDATE_INTERVAL = 20

ENERGY = {
    "kitchen_sm1": "sub_metering_1",
    "laundry_sm2": "sub_metering_2",
    "heater_ac_sm3": "sub_metering_3",
    "other": "other_wh",
    "total": "total_wh",
}

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BROKER,
    group_id=GROUP_ID,
    auto_offset_reset="earliest", # Reads earliest undread message possible
    enable_auto_commit=True, # Sets offsets periodically
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)


total_messages = 0
update_amount = 1

totals = {
    hour: {
        "activePower": 0,
        "meter1": 0,
        "meter2": 0,
        "meter3": 0,
        "count": 0
    }
    for hour in range(24)
}

# Which hours of the day use the most power, and which sub-meter drives it?
def display_results():

    print("=== Q2: average active power by hour of day (highest first) ===", flush=True)
    print(f"Time since consuming: {last_update} seconds. Update number {update_amount}", flush=True)

    sorted_hours = sorted(totals.items(), key = lambda item: item[1]["activePower"] / item[1]["count"], reverse = True) # Descending order

    for hour, values in sorted_hours:
        count = values["count"]
        activePower = values["activePower"] / count
        meter1 = values["meter1"] / count
        meter2 = values["meter2"] / count
        meter3 = values["meter3"] / count
        print(f"Hour {hour} | Active Power {activePower} | Sub-Meter 1 {meter1} | Sub-Meter 2 {meter2} | Sub-Meter 3 {meter3}", flush= True)
    
    print("\n", flush=True)

last_update = time.monotonic()
start_time = time.monotonic()

# Which hours of the day use the most power, and which sub-meter drives it?
for message in consumer:

    if message.key == b"END":
        print("Producer end message received. Stopping")
        break

    row = message.value

    timestamp = row["ts"]
    hour = int(timestamp[11:13])  # "%Y-%m-%d %H:%M:%S". Ex: 2026-11-17 14:30:45

    activePower = float(row["global_active_power"]) # kW
    # totalWh = activePower * 1000 / 60
    meter1 = float(row["sub_metering_1"]) # Wh
    meter2 = float(row["sub_metering_2"]) # Wh
    meter3 = float(row["sub_metering_3"]) # Wh

    other = activePower - meter1 - meter2 - meter3

    # Update hourly totals
    totals[hour]["meter1"] += meter1
    totals[hour]["meter2"] += meter2
    totals[hour]["meter3"] += meter3
    totals[hour]["activePower"] += activePower
    totals[hour]["count"] += 1

    total_messages += 1

    # Only every 20 seconds
    now = time.monotonic()
    if now - last_update >= UPDATE_INTERVAL:        
        update_amount += 1
        last_update = now
        display_results()

update_amount += 1
last_update = now
display_results()
consumer.close()

print("\n=== Consumer summary ===")
print(f"Total: consumed {total_messages:,} rows.")

time_taken = time.monotonic() - start_time
print(f"Time taken: {time_taken:.2f} seconds")
print(f"Amount of updates: {update_amount}")
