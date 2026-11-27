"""The thresholds in our three questions, in one place.

Used by the Spark job (jobs/batch_queries.py), the database queries (serve/queries.py) and the
Part 4 experiment (quality/measure.py), so the batch answers, the served answers and the
measurements always use the same values.
"""
PF_THRESHOLD = 0.95  # Q3: an hour is "low power factor" when its power factor is below this
MIN_RUN_HOURS = 2    # Q3b: a low power-factor period must last at least this many consecutive hours
