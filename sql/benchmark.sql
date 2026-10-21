-- BigQuery storage optimization benchmark.
-- Replace PROJECT_ID with your Google Cloud project ID before running.
-- Run scripts/benchmark_bq.py to create the copies and dry-run these queries.

CREATE OR REPLACE TABLE `PROJECT_ID.f1_analytics.agg_driver_race_summary_unpartitioned` AS
SELECT * FROM `PROJECT_ID.f1_analytics.agg_driver_race_summary`;

CREATE OR REPLACE TABLE `PROJECT_ID.f1_analytics.agg_tyre_degradation_unpartitioned` AS
SELECT * FROM `PROJECT_ID.f1_analytics.agg_tyre_degradation`;

CREATE OR REPLACE TABLE `PROJECT_ID.f1_analytics.agg_pit_strategy_unpartitioned` AS
SELECT * FROM `PROJECT_ID.f1_analytics.agg_pit_strategy`;

CREATE OR REPLACE TABLE `PROJECT_ID.f1_analytics.agg_weather_impact_unpartitioned` AS
SELECT * FROM `PROJECT_ID.f1_analytics.agg_weather_impact`;

CREATE OR REPLACE TABLE `PROJECT_ID.f1_analytics.agg_race_results_unpartitioned` AS
SELECT * FROM `PROJECT_ID.f1_analytics.agg_race_results`;

-- Query: Tyre degradation for one race
SELECT *
FROM `PROJECT_ID.f1_analytics.agg_tyre_degradation`
WHERE session_key = 9472;

-- Query: Pit strategy for one race and driver
SELECT *
FROM `PROJECT_ID.f1_analytics.agg_pit_strategy`
WHERE session_key = 9472 AND driver_number = 1;

-- Query: Weather impact for one race
SELECT *
FROM `PROJECT_ID.f1_analytics.agg_weather_impact`
WHERE session_key = 9472;

-- Query: Driver summary for one team
SELECT *
FROM `PROJECT_ID.f1_analytics.agg_driver_race_summary`
WHERE team_name = 'Red Bull Racing';
