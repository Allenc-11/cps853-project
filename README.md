# CPS853 Team 5 — Household Power Consumption

Spark turns one household's electricity readings (2 million rows, 2006–2010) into
Parquet and answers our 3 questions. Kafka replays the same raw file as a stream, bad readings
included, PostgreSQL serves the batch and stream results, and quality checks on both paths set bad
rows aside, count rows at every stage, and raise alerts.

## Before you start

1. Install **Docker Desktop** and open it. Wait for **"Engine running"**.
2. Download the dataset from https://doi.org/10.24432/C58K54, unzip it, and put
   `household_power_consumption.txt` in the `data\raw\` folder.
3. Open PowerShell **in this project folder**.

## Run

    docker compose up

This runs two paths side by side, both starting from the raw file:
- batch: **ingest** (raw file → Parquet), then **batch-queries** (answers Q1–Q3), then **load-batch** (results → PostgreSQL);
- stream: **producer** replays the raw file into Kafka, and **consumer** checks every message and saves running totals to PostgreSQL.

When the stream ends (about 5 minutes), **serve-queries** answers Q1–Q3 from PostgreSQL with response times,
and **quality-report** shows rows in and out at every stage, the quality checks, and any alerts.
The first run downloads Spark (about 1 GB) and builds a small Python image. Ignore the `INFO` and `WARN` lines.
`postgres` and `kafka` keep running afterwards; stop everything with Ctrl+C or `docker compose down`.

You should see, from `ingest`:

    rows in (raw file):      2075259
    rows out (Parquet):      2049270
    rows quarantined:        25989
    in == out + quarantined: True

and a table of quality checks (25,979 rows with every measurement missing, 10 where the
sub-meters add up to more than the total) ending `No alert`.

then, from `batch-queries`, tables for Q1, Q2 and Q3, each with a run time,
and from `load-batch` a row count per table with `match True` for all 7 tables.
While it runs, `consumer` prints `ALERT` lines for 8 windows of 10,000 messages where the meter
recorded nothing for hours or days (see Part 4). At the end it prints

    All runs:  consumed 2,075,259 messages: 2,049,270 applied, 25,989 set aside, 0 late or repeated.
    in == applied + set aside + late: True

the same rows kept and set aside as `ingest`, found by the stream's own checks. `serve-queries`
prints the answers, `hours_that_differ` = 0 (stream and batch agree hour by hour), and a
response-time table. `quality-report` shows every stage `balances True`, both paths `equal`, and
the stream alerts (8 windows, each for `missing_measurement` and for `all checks`).
Each of these ends with `exited with code 0`.

**Adding your part:** add your services to `docker-compose.yml` under your section.

## Where things go

| Folder | What's in it |
|---|---|
| `docker-compose.yml` | Starts the whole system |
| `jobs/` | The Spark scripts |
| `streaming/` | The Kafka scripts |
| `serve/` | Part 3: database schema, batch loader, the queries for Q1–Q3 |
| `quality/` | Part 4: the check rules, the quality report, and the corrupted-data experiment |
| `data/raw/` | The dataset you downloaded, read by both `ingest` and `producer` |
| `data/parquet/power/` | Clean data in Parquet, one folder per year |
| `data/quarantine/ingest/` | Bad rows, set aside (not deleted), with a `reject_reason` |
| `data/results/` | Answers to Q1–Q3, for the database (Part 3), and `serve_timings.csv` (response times) |

`data/` is not in git.

## Our 3 questions

1. Where is most of the power used, and how does it change by month and season?
2. Which hours of the day use the most power, and why?
3. How efficient is the household's power use (power factor)? Are there periods of 2+ hours where the power factor is below 0.95?

## Columns in the Parquet data (use these names in Parts 2–4)

`ts` (timestamp), `global_active_power` (kW), `global_reactive_power` (kW), `voltage` (V),
`global_intensity` (A), `sub_metering_1` kitchen, `sub_metering_2` laundry,
`sub_metering_3` water heater + A/C (Wh per minute), `year`.

## Part 3: querying the database

PostgreSQL (database `power`, user `power`, password `power`) holds:

| Table | Filled by | What's in it |
|---|---|---|
| `q1_share`, `q1_seasonal`, `q1_monthly`, `q2_hourly`, `q3_hourly`, `q3_runs` | `load-batch` | The Spark results from `data/results/` |
| `quarantine_ingest` | `load-batch` | Rows ingest set aside, with `reject_reason` |
| `stream_hourly` | `consumer` | Running totals per hour of event time, updated every 5 s |
| `stream_hour_of_day` | `consumer` | The same totals by hour of day (24 rows), for the live Q2 |
| `stream_progress` | `consumer` | Kafka offset reached, and messages in / applied / set aside / late |
| `stream_rejects` | `consumer` | Stream messages that could not be used, with the reason (25,989 in a normal run: the same readings as `quarantine_ingest`) |

Answer the questions again, with response times (works mid-stream too):

    docker compose run --rm --no-deps serve-queries

Watch the stream arrive, or ask your own question:

    docker compose exec postgres psql -U power -d power -c "select * from stream_progress"
    docker compose exec postgres psql -U power -d power

From a desktop client (DBeaver, pgAdmin), connect to `localhost:5433`.

Add a bad row to the stream by hand (it ends up in `stream_rejects`):

    echo 'this is not json' | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic power-usage

The consumer saves its offset with its totals, so `docker compose stop consumer` then
`docker compose up --no-deps consumer` carries on from where it stopped without losing or repeating rows.
(Use `--no-deps` so that only the consumer starts.)
To start again from an empty database: `docker compose down -v`.

## Part 4: quality checks and monitoring

Both paths read the raw file, and the same checks run on the batch path (`jobs/ingest.py`, Spark)
and the stream path (`streaming/consumer.py`), with the limits in one place, `quality/rules.py`.
So both set aside the same 25,989 readings and keep the same 2,049,270, which `quality-report`
and `serve-queries` check:

| Check | Batch | Stream | Catches |
|---|---|---|---|
| Wrong schema | `unparseable_timestamp` | `unreadable_message`, `unparseable_timestamp` | Text that is not a reading |
| Missing values | `all_measurements_missing`, `missing_measurement` | `missing_measurement` | Empty or non-numeric values |
| Impossible range | `impossible_power`, `voltage_out_of_range` | same | Negative readings, over 12 kW, voltage outside 200–260 V |
| Inconsistent | `submeters_exceed_total` | same | Sub-meters adding up to more than the total energy |
| Duplicates | `duplicate_timestamp` | `late_or_duplicate` | A second reading for the same minute |

Rows that fail are **set aside, not dropped**: `quarantine_ingest` (batch) and
`stream_rejects` (stream), with the check they failed. An **alert** is raised (printed and
saved in `alerts`) when more than 2% of rows fail: per run for the batch, per 10,000
messages for the stream. The real data misses 1.25% of its minutes, so the batch raises no
alert, but the missing minutes come in long gaps (the meter recorded nothing for hours or days),
so 8 of the stream's 207 windows go above 2% and raise an alert in a normal run. Rows in and out at every stage are in `pipeline_counts`, and
rows failed per check in `quality_checks`. To see it all:

    docker compose run --rm --no-deps quality-report

To raise an alert on purpose, replay the stream with 5% corrupted messages added:

    cat quality/reset_stream.sql | docker compose exec -T postgres psql -U power -d power
    docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic power-usage
    docker compose run --rm --no-deps -e CORRUPT_RATE=0.05 producer
    docker compose run --rm --no-deps consumer

The producer marks each corrupted copy with an `injected` Kafka header (the checks ignore it),
so the experiment counts what the checks caught among the copies apart from the real bad readings.
The experiment behind the report's numbers (checks on and off, 1% and 5% corrupted, about
15 minutes; Git Bash or WSL on Windows) writes `data/quality/experiment.csv`:

    bash quality/run_experiment.sh

## If something goes wrong

| Problem | Fix |
|---|---|
| `Cannot connect to the Docker daemon` | Open Docker Desktop and wait for "Engine running". |
| `Path does not exist: .../data/raw/...` | The dataset isn't in `data\raw\`, or you're not in the project folder. |
| `ingest` exits with an error code | Scroll up to the first `ERROR` line and fix that first; `batch-queries` won't start. |
| `port is already allocated` for 5433 | Something else uses that port. Change `"5433:5432"` in `docker-compose.yml`. |
| `relation "..." does not exist` or `column "..." does not exist` | The database was created before `serve/schema.sql` changed. Run `docker compose down -v`, then `docker compose up`. |
