"""Part 4: the data-quality rules, shared by the batch job and the stream consumer.

jobs/ingest.py (Spark) and streaming/consumer.py (Python) apply the same checks with the same
limits, so a reading is set aside the same way whichever path it takes. The checks run in the
order listed; a row that fails is set aside under the first check it fails, so the counts per
check add up to the rows set aside.

The limits come from the house's physics, not from statistics, so that a real but unusual
minute (a high-demand peak, a high-voltage evening) is kept, and only readings that cannot be
real are set aside. Measured on the full dataset (2026-11-26):
- voltage is 223.2-254.15 V. The supply standard (230 V +/- 10%, 207-253 V) would set aside 57
  genuine readings, so the check uses a meter-fault band instead: 200-260 V.
- active power peaks at 11.122 kW; a 12 kVA supply (common in France) cannot deliver more than 12 kW.
- the three sub-meters are whole Wh each, so rounding alone can push their sum up to 1.5 Wh
  above the total energy. Anything beyond that is impossible: 10 readings.
"""

VOLTAGE_MIN = 200.0  # V
VOLTAGE_MAX = 260.0  # V
MAX_ACTIVE_KW = 12.0  # kW
SUBMETER_ROUNDING_WH = 1.5  # Wh: 3 sub-meters x 0.5 Wh rounding each

# Raise an alert when more than this share of the rows checked fails one check, or all checks
# together. The real data loses 1.25% of its minutes to missing readings, so 2% sits above the
# normal level and catches a real deterioration.
ALERT_THRESHOLD = 0.02
STREAM_ALERT_WINDOW = 10_000  # the stream is judged in windows of this many messages

# Stream only: a reading up to this much older (in event time) than the newest one applied is
# still added to its own hour; an older one is set aside as too late. The producer's late readings
# arrive 1,000 readings (1,000 minutes) late, so a day leaves room. The consumer remembers the
# minutes it applied within this window, to count a duplicate only once.
ALLOWED_LATENESS_MINUTES = 1440

MEASURES = ("global_active_power", "global_reactive_power", "voltage", "global_intensity",
            "sub_metering_1", "sub_metering_2", "sub_metering_3")

# Check name -> what it catches, in the order the batch job applies them.
BATCH_CHECKS = {
    "unparseable_timestamp": "wrong schema: the date and time cannot be read",
    "all_measurements_missing": "missing values: every measurement is missing",
    "missing_measurement": "missing values: at least one measurement is missing",
    "impossible_power": "impossible range: a negative reading, or active power above 12 kW",
    "voltage_out_of_range": "impossible range: voltage outside 200-260 V",
    "submeters_exceed_total": "inconsistent: the sub-meters add up to more than the total energy",
    "duplicate_timestamp": "duplicates: a second reading for the same minute",
}

# The same checks in the stream, in the order the consumer applies them.
STREAM_CHECKS = {
    "unreadable_message": "wrong schema: the message is not a JSON object",
    "unparseable_timestamp": "wrong schema: the time cannot be read",
    "missing_measurement": "missing values: a measurement is missing or not a number",
    "impossible_power": BATCH_CHECKS["impossible_power"],
    "voltage_out_of_range": BATCH_CHECKS["voltage_out_of_range"],
    "submeters_exceed_total": BATCH_CHECKS["submeters_exceed_total"],
    "late_or_duplicate": "duplicates: a minute already applied, or more than a day older than the newest reading",
}


def range_check(active_kw, reactive_kw, voltage, intensity, sm1, sm2, sm3):
    """Return the first range check a complete reading fails, or None. Mirrors jobs/ingest.py."""
    if min(active_kw, reactive_kw, intensity, sm1, sm2, sm3) < 0 or active_kw > MAX_ACTIVE_KW:
        return "impossible_power"
    if not VOLTAGE_MIN <= voltage <= VOLTAGE_MAX:
        return "voltage_out_of_range"
    if sm1 + sm2 + sm3 > active_kw * 1000 / 60 + SUBMETER_ROUNDING_WH:
        return "submeters_exceed_total"
    return None


def alerts(failures, rows_checked):
    """Checks whose failure rate is above ALERT_THRESHOLD: [(check, rows failed, rate)].

    failures: {check name: rows failed}. The total over all checks is judged too, as "all checks".
    """
    if not rows_checked:
        return []
    judged = dict(failures, **{"all checks": sum(failures.values())})
    return [(name, failed, failed / rows_checked) for name, failed in judged.items()
            if failed / rows_checked > ALERT_THRESHOLD]
