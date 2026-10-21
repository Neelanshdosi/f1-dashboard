-- =====================================================================
-- BigQuery DDL for F1 Race Strategy Analytics (Gold Layer)
-- Dataset: f1_analytics
-- Queries filter by one race's session_key, so range partitioning on that key
-- lets BigQuery prune unrelated sessions. Bounds match config.yaml
-- bigquery.session_key_partition (9000 through 12000, interval 1).
-- =====================================================================

-- 1. Driver Race Summary
CREATE TABLE IF NOT EXISTS `f1_analytics.agg_driver_race_summary` (
    session_key INT64,
    meeting_key INT64,
    driver_number INT64,
    driver_name STRING,
    name_acronym STRING,
    team_name STRING,
    team_colour STRING,
    total_laps INT64,
    fastest_lap_seconds FLOAT64,
    avg_lap_seconds FLOAT64,
    total_pit_stops INT64,
    total_pit_duration FLOAT64,
    starting_compound STRING,
    max_stint_length INT64
)
PARTITION BY RANGE_BUCKET(session_key, GENERATE_ARRAY(9000, 12000, 1))
CLUSTER BY driver_number, team_name;


-- 2. Tyre Degradation Analysis
CREATE TABLE IF NOT EXISTS `f1_analytics.agg_tyre_degradation` (
    session_key INT64,
    driver_number INT64,
    driver_name STRING,
    team_name STRING,
    stint_number INT64,
    compound STRING,
    tyre_age INT64,
    lap_number INT64,
    lap_time_seconds FLOAT64,
    stint_avg_lap_time FLOAT64,
    delta_to_stint_avg FLOAT64
)
PARTITION BY RANGE_BUCKET(session_key, GENERATE_ARRAY(9000, 12000, 1))
CLUSTER BY compound, driver_number;


-- 3. Pit Stop & Stint Strategy
CREATE TABLE IF NOT EXISTS `f1_analytics.agg_pit_strategy` (
    session_key INT64,
    driver_number INT64,
    driver_name STRING,
    team_name STRING,
    stint_number INT64,
    compound STRING,
    lap_start INT64,
    lap_end INT64,
    stint_length INT64,
    pit_lap INT64,
    pit_duration FLOAT64,
    stop_duration FLOAT64
)
PARTITION BY RANGE_BUCKET(session_key, GENERATE_ARRAY(9000, 12000, 1))
CLUSTER BY driver_number, compound;


-- 4. Weather & Track Condition Impact
CREATE TABLE IF NOT EXISTS `f1_analytics.agg_weather_impact` (
    session_key INT64,
    date_utc TIMESTAMP,
    air_temperature FLOAT64,
    track_temperature FLOAT64,
    humidity FLOAT64,
    pressure FLOAT64,
    wind_speed FLOAT64,
    wind_direction INT64,
    rainfall INT64,
    field_avg_lap_time FLOAT64
)
PARTITION BY RANGE_BUCKET(session_key, GENERATE_ARRAY(9000, 12000, 1))
CLUSTER BY session_key;


-- 5. Driver Telemetry Analysis
CREATE TABLE IF NOT EXISTS `f1_analytics.agg_driver_telemetry` (
    session_key INT64,
    meeting_key INT64,
    driver_number INT64,
    driver_name STRING,
    team_name STRING,
    team_colour STRING,
    max_speed INT64,
    avg_speed FLOAT64,
    avg_throttle FLOAT64,
    full_throttle_pct FLOAT64,
    brake_pct FLOAT64,
    drs_open_pct FLOAT64
)
PARTITION BY RANGE_BUCKET(session_key, GENERATE_ARRAY(9000, 12000, 1))
CLUSTER BY driver_number;


-- 6. Official Race Results & Starting Grid
CREATE TABLE IF NOT EXISTS `f1_analytics.agg_race_results` (
    session_key INT64,
    driver_number INT64,
    driver_name STRING,
    name_acronym STRING,
    team_name STRING,
    team_colour STRING,
    grid_position INT64,
    final_position INT64,
    positions_gained INT64,
    status STRING,
    total_pit_stops INT64,
    number_of_laps INT64,
    points FLOAT64,
    duration_seconds FLOAT64,
    gap_to_leader_seconds FLOAT64,
    laps_down INT64,
    grid_position_missing BOOLEAN
)
PARTITION BY RANGE_BUCKET(session_key, GENERATE_ARRAY(9000, 12000, 1))
CLUSTER BY driver_number, team_name;


-- 6. Pipeline Run Audit Log
CREATE TABLE IF NOT EXISTS `f1_analytics.pipeline_run_audit` (
    job_name STRING,
    session_key INT64,
    season INT64,
    table_name STRING,
    rows_in INT64,
    rows_out INT64,
    rows_rejected INT64,
    started_at TIMESTAMP,
    duration_seconds FLOAT64,
    status STRING
);


-- 7. Sessions Dimension (unpartitioned)
CREATE TABLE IF NOT EXISTS `f1_analytics.dim_sessions` (
    session_key INT64,
    meeting_key INT64,
    circuit_key INT64,
    circuit_short_name STRING,
    country_name STRING,
    country_code STRING,
    location STRING,
    session_type STRING,
    session_name STRING,
    meeting_name STRING,
    date_start TIMESTAMP,
    date_end TIMESTAMP,
    gmt_offset STRING,
    year INT64,
    season INT64
);



