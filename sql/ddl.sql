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
