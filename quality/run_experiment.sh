#!/usr/bin/env bash
# Part 4 experiment: replay the stream with corrupted messages added on purpose, with the
# quality checks on and off, and measure each run against the clean batch results.
# Results: data/quality/experiment.csv (one row per run).
#
# Run from the project folder, after docker compose up has finished once:
#   bash quality/run_experiment.sh
# (on Windows: Git Bash or WSL). Each run replays all 2 million readings: about 3-4 minutes.
# The last run is a normal one (checks on, nothing corrupted), so the database ends up as after
# docker compose up.
set -euo pipefail
export MSYS_NO_PATHCONV=1  # Git Bash on Windows: pass /opt/kafka/... to docker unchanged

SPEED=${SPEED:-50000}   # producer messages per second; the consumer then reads the full topic
RUNS=("on 0.01" "on 0.05" "off 0.01" "off 0.05" "on 0")

psql() { docker compose exec -T postgres psql -U power -d power -q "$@"; }
kafka_topics() { docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "$@"; }

rm -f data/quality/experiment.csv
for run in "${RUNS[@]}"; do
  read -r checks rate <<<"$run"
  label="checks $checks, ${rate} corrupted"
  echo; echo "########## $label ##########"

  psql < quality/reset_stream.sql
  if kafka_topics --list | grep -qx power-usage; then
    kafka_topics --delete --topic power-usage
    while kafka_topics --list | grep -qx power-usage; do sleep 1; done
  fi

  # Producer first and in full, so the consumer's time measures its own work, not waiting.
  # Duplicates and late delivery off, so each run measures the corruption alone.
  docker compose run --rm --no-deps -e SPEED="$SPEED" -e CORRUPT_RATE="$rate" \
    -e DUPLICATE_EVERY=0 -e LATE_EVERY=0 producer | grep -v '^Sent '
  log="data/quality/consumer_checks-${checks}_rate-${rate}.log"
  docker compose run --rm --no-deps -e QUALITY_CHECKS="$checks" consumer > "$log"
  grep -m 3 ALERT "$log" || echo "(no alerts)"
  sed -n '/=== Consumer summary ===/,$p' "$log"
  docker compose run --rm --no-deps -e RUN_LABEL="$label" quality-measure
done
echo; echo "Results: data/quality/experiment.csv"
