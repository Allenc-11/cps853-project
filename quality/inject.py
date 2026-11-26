"""Part 4 experiment: add corrupted messages to the stream on purpose, to test the checks.

The producer (streaming/producer.py) calls corrupt() after a share of the real readings
(CORRUPT_RATE), and sends the corrupted copy as an extra message straight after the real one.
The real reading is never changed, so with the checks on the stream should still match the
batch results exactly; with the checks off, the corrupted copies change the answers.

Each corrupted copy breaks one rule, in turn, so every stream check gets the same number of
test messages and the counts caught can be compared kind by kind with the counts injected.
Only valid readings are copied (can_corrupt), so that each copy breaks exactly the one rule
meant for it: a copy of a reading that already has a missing value would be caught as missing,
whatever was done to it. The producer marks each copy with an "injected" Kafka header, which the
consumer uses only to count the copies' catches apart from the real bad readings.
"""
import json
import math

from quality.rules import MEASURES, range_check

# Kind of corruption -> the stream check (quality/rules.py STREAM_CHECKS) expected to catch it.
KINDS = {
    "broken_json": "unreadable_message",
    "bad_timestamp": "unparseable_timestamp",
    "missing_value": "missing_measurement",
    "negative_power": "impossible_power",
    "voltage_spike": "voltage_out_of_range",
    "submeter_overflow": "submeters_exceed_total",
    "repeated_reading": "late_or_duplicate",
}


def can_corrupt(row):
    """True for a reading that passes every check: all measurements present and in range."""
    values = [row.get(name) for name in MEASURES]
    if not all(isinstance(v, float) and math.isfinite(v) for v in values):
        return False
    return range_check(*values) is None


def corrupt(row, n):
    """Return (kind, message bytes) for the n-th corrupted copy of `row` (a dict ready for JSON)."""
    kind = list(KINDS)[n % len(KINDS)]
    bad = dict(row)
    if kind == "broken_json":
        return kind, json.dumps(bad).encode("utf-8")[:-7]  # cut off mid-message
    if kind == "bad_timestamp":
        bad["ts"] = "not a time"
    elif kind == "missing_value":
        bad["global_active_power"] = None
    elif kind == "negative_power":
        bad["global_active_power"] = -abs(bad["global_active_power"]) - 1
    elif kind == "voltage_spike":
        bad["voltage"] = 400.0
    elif kind == "submeter_overflow":
        bad["sub_metering_3"] = bad["global_active_power"] * 1000 / 60 + 50
    # repeated_reading: an exact copy, so only its event time gives it away
    return kind, json.dumps(bad).encode("utf-8")
