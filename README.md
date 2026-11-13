# CPS853 Team 5 — Household Power Consumption

Spark turns one household's electricity readings (2 million rows, 2006–2010) into
Parquet and answers our 3 questions.

## Before you start

1. Install **Docker Desktop** and open it. Wait for **"Engine running"**.
2. Download the dataset from https://doi.org/10.24432/C58K54, unzip it, and put
   `household_power_consumption.txt` in the `data\raw\` folder.
3. Open PowerShell **in this project folder**.

## Run

    docker compose up

This runs Part 1 in order: **ingest** (raw file → Parquet), then **batch-queries** (answers Q1–Q3).
The first run downloads Spark (about 1 GB). Ignore the `INFO` and `WARN` lines.

You should see, from `ingest`:

    rows in (raw file):      2075259
    rows out (Parquet):      2049280
    rows quarantined:        25979
    in == out + quarantined: True

then, from `batch-queries`, tables for Q1, Q2 and Q3, each with a run time.
Both end with `exited with code 0`.

**Adding your part:** add your services to `docker-compose.yml` under your section.

## Where things go

| Folder | What's in it |
|---|---|
| `docker-compose.yml` | Starts the whole system |
| `jobs/` | The Spark scripts |
| `data/raw/` | The dataset you downloaded |
| `data/parquet/power/` | Clean data in Parquet, one folder per year |
| `data/quarantine/ingest/` | Bad rows, set aside (not deleted), with a `reject_reason` |
| `data/results/` | Answers to Q1–Q3, for the database (Part 3) |

`data/` is not in git.

## Our 3 questions

1. Where is most of the power used, and how does it change by month and season?
2. Which hours of the day use the most power, and why?
3. How efficient is the household's power use (power factor)? Are there periods of 2+ hours where the power factor is below 0.95?

## Columns in the Parquet data (use these names in Parts 2–4)

`ts` (timestamp), `global_active_power` (kW), `global_reactive_power` (kW), `voltage` (V),
`global_intensity` (A), `sub_metering_1` kitchen, `sub_metering_2` laundry,
`sub_metering_3` water heater + A/C (Wh per minute), `year`.

## If something goes wrong

| Problem | Fix |
|---|---|
| `Cannot connect to the Docker daemon` | Open Docker Desktop and wait for "Engine running". |
| `Path does not exist: .../data/raw/...` | The dataset isn't in `data\raw\`, or you're not in the project folder. |
| `ingest` exits with an error code | Scroll up to the first `ERROR` line and fix that first; `batch-queries` won't start. |
