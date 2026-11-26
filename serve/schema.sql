-- Part 3: PostgreSQL schema. Postgres runs this once, when its data volume is first created
-- (mounted into /docker-entrypoint-initdb.d). To start again from empty: docker compose down -v
--
-- Timestamps are "timestamp without time zone": the readings are wall-clock times from one
-- house, and every stage (Spark, Kafka, Postgres) keeps them as written, with no zone shift.

-- ---------- Batch results (loaded from data/results/ by serve/load_batch.py) ----------

-- Q1: share of active energy by category over the whole period.
CREATE TABLE q1_share (
    category     text PRIMARY KEY,
    kwh          double precision NOT NULL,
    pct_of_total double precision NOT NULL
);

-- Q1: kWh per observed day, by season.
CREATE TABLE q1_seasonal (
    season                    text PRIMARY KEY,
    minutes_observed          bigint NOT NULL,
    kitchen_sm1_kwh_per_day   double precision,
    laundry_sm2_kwh_per_day   double precision,
    heater_ac_sm3_kwh_per_day double precision,
    other_kwh_per_day         double precision,
    total_kwh_per_day         double precision
);

-- Q1: kWh per observed day, by month ('yyyy-MM').
CREATE TABLE q1_monthly (
    month                     text PRIMARY KEY,
    minutes_observed          bigint NOT NULL,
    kitchen_sm1_kwh_per_day   double precision,
    laundry_sm2_kwh_per_day   double precision,
    heater_ac_sm3_kwh_per_day double precision,
    other_kwh_per_day         double precision,
    total_kwh_per_day         double precision
);

-- Q2: average power and sub-meter energy for each hour of the day (0-23).
CREATE TABLE q2_hourly (
    hour                         integer PRIMARY KEY,
    avg_active_kw                double precision,
    kitchen_sm1_avg_wh_per_min   double precision,
    laundry_sm2_avg_wh_per_min   double precision,
    heater_ac_sm3_avg_wh_per_min double precision,
    other_avg_wh_per_min         double precision
);

-- Q3: one row per clock hour. p and q are the hour's summed active and reactive power (kW).
CREATE TABLE q3_hourly (
    hour_start    timestamp PRIMARY KEY,
    p             double precision,
    q             double precision,
    avg_active_kw double precision,
    minutes       bigint NOT NULL,
    power_factor  double precision,
    low_pf        boolean
);

-- Q3: runs of 2+ consecutive hours with power factor below 0.95.
CREATE TABLE q3_runs (
    run_start        timestamp PRIMARY KEY,
    run_last_hour    timestamp NOT NULL,
    hours            bigint NOT NULL,
    avg_power_factor double precision,
    min_power_factor double precision,
    avg_active_kw    double precision
);

-- Rows that ingest set aside (data/quarantine/ingest), kept in their own table. The stream sets
-- the same raw readings aside on its own, in stream_rejects.
CREATE TABLE quarantine_ingest (
    id                    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    raw_date              text,
    raw_time              text,
    ts                    timestamp,
    global_active_power   double precision,
    global_reactive_power double precision,
    voltage               double precision,
    global_intensity      double precision,
    sub_metering_1        double precision,
    sub_metering_2        double precision,
    sub_metering_3        double precision,
    reject_reason         text NOT NULL
);

-- ---------- Stream results (written live by streaming/consumer.py) ----------

-- Running totals per event-time hour. The same grain as q3_hourly, so the stream can answer
-- Q2 (group by hour of day) and Q3 (power factor per hour) while rows are still arriving.
CREATE TABLE stream_hourly (
    hour_start      timestamp PRIMARY KEY,
    minutes         integer NOT NULL,
    active_kw_sum   double precision NOT NULL,
    reactive_kw_sum double precision NOT NULL,
    sm1_wh          double precision NOT NULL,
    sm2_wh          double precision NOT NULL,
    sm3_wh          double precision NOT NULL,
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- Where the consumer has got to, saved in the same transaction as stream_hourly, so a
-- restarted consumer carries on from exactly the last saved message.
-- messages_in = rows_applied + rows_rejected + rows_late (the END marker is not counted).
-- rows_late: duplicates and readings later than the allowed lateness (set aside, counted).
-- late_applied: of rows_applied, readings that arrived out of order but within the allowed lateness.
-- recent_minutes: the minutes applied within the allowed lateness, so a restarted consumer still
-- recognises a duplicate.
CREATE TABLE stream_progress (
    topic           text    NOT NULL,
    kafka_partition integer NOT NULL,
    next_offset     bigint  NOT NULL,
    last_ts         timestamp,
    messages_in     bigint  NOT NULL,
    rows_applied    bigint  NOT NULL,
    rows_rejected   bigint  NOT NULL,
    rows_late       bigint  NOT NULL,
    late_applied    bigint  NOT NULL DEFAULT 0,
    recent_minutes  timestamp[] NOT NULL DEFAULT '{}',
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, kafka_partition)
);

-- Stream messages the consumer could not use, set aside with the reason.
CREATE TABLE stream_rejects (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kafka_offset bigint NOT NULL,
    reason       text   NOT NULL,
    raw_value    text,
    rejected_at  timestamptz NOT NULL DEFAULT now()
);

-- ---------- Part 4: quality and monitoring ----------

-- Rows in and rows out at every stage. Batch stages are loaded by serve/load_batch.py from
-- data/quality/*.json; the producer and consumer rows are written by streaming/consumer.py.
-- For every stage: rows_in + rows_added = rows_out + rows_set_aside. rows_added is only used by
-- the producer, for corrupted messages it adds on purpose in the Part 4 experiment.
CREATE TABLE pipeline_counts (
    stage          text PRIMARY KEY,
    stage_order    integer NOT NULL,
    rows_in        bigint  NOT NULL,
    rows_added     bigint  NOT NULL DEFAULT 0,
    rows_out       bigint  NOT NULL,
    rows_set_aside bigint  NOT NULL,
    seconds        double precision,
    note           text,
    updated_at     timestamptz NOT NULL DEFAULT now()
);

-- Rows failed per check and stage (quality/rules.py). A row is counted under the first check it fails.
-- rows_failed_injected: of rows_failed, the corrupted copies added on purpose (Part 4 experiment, stream only).
CREATE TABLE quality_checks (
    stage        text   NOT NULL,
    check_order  integer NOT NULL,
    check_name   text   NOT NULL,
    rows_checked bigint NOT NULL,
    rows_failed  bigint NOT NULL,
    rows_failed_injected bigint NOT NULL DEFAULT 0,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (stage, check_name)
);

-- Every time a check (or all checks together) failed on more than the threshold share of rows:
-- once per batch stage, and once per window of messages in the stream.
CREATE TABLE alerts (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stage        text   NOT NULL,
    check_name   text   NOT NULL,
    rows_checked bigint NOT NULL,
    rows_failed  bigint NOT NULL,
    fail_rate    double precision NOT NULL,
    threshold    double precision NOT NULL,
    detail       text,
    raised_at    timestamptz NOT NULL DEFAULT now()
);

-- Part 4 experiment: corrupted messages the producer added on purpose, by kind, from its END
-- message (quality/inject.py), so the checks' catches can be compared with what was injected.
CREATE TABLE injected_messages (
    kind           text PRIMARY KEY,
    expected_check text   NOT NULL,
    messages       bigint NOT NULL
);
