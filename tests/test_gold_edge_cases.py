"""Edge cases and optional-column behavior for Gold aggregations."""

from pyspark.sql import SparkSession

from spark_jobs.silver_to_gold import (
    compute_driver_race_summary,
    compute_pit_strategy,
    compute_tyre_degradation,
    compute_weather_impact,
)


def test_driver_summary_ignores_out_laps_and_null_times(spark: SparkSession):
    laps = spark.createDataFrame(
        [
            {"session_key": 9472, "driver_number": 1, "lap_number": 1, "lap_time_seconds": 95.0, "is_pit_out_lap": False},
            {"session_key": 9472, "driver_number": 1, "lap_number": 2, "lap_time_seconds": 90.0, "is_pit_out_lap": True},
            {"session_key": 9472, "driver_number": 1, "lap_number": 3, "lap_time_seconds": None, "is_pit_out_lap": False},
        ]
    )
    drivers = spark.createDataFrame([{"session_key": 9472, "driver_number": 1, "full_name": "Driver"}])
    stints = spark.createDataFrame(
        [{"session_key": 9472, "driver_number": 1, "stint_number": 1, "lap_start": 1, "lap_end": 3}]
    )
    pit = spark.createDataFrame([{"session_key": 9472, "driver_number": 1, "lap_number": 3}])

    row = compute_driver_race_summary(laps, drivers, stints, pit).first()

    assert row["fastest_lap_seconds"] == 95.0
    assert row["avg_lap_seconds"] == 95.0
    assert row["total_pit_duration"] == 0.0
    assert row["team_name"] == ""


def test_tyre_degradation_defaults_missing_out_lap_flag_and_deduplicates(spark: SparkSession):
    lap = {
        "session_key": 9472,
        "driver_number": 1,
        "stint_number": 1,
        "tyre_compound": "SOFT",
        "tyre_life": 5,
        "lap_number": 5,
        "lap_time_seconds": 96.0,
    }
    laps = spark.createDataFrame([lap, lap])
    drivers = spark.createDataFrame([{"session_key": 9472, "driver_number": 1}])
    stints = spark.createDataFrame(
        [{"session_key": 9472, "driver_number": 1, "stint_number": 1}]
    )

    rows = compute_tyre_degradation(laps, drivers, stints).collect()

    assert len(rows) == 1
    assert rows[0]["tyre_age"] == 5
    assert rows[0]["driver_name"] == ""


def test_pit_strategy_defaults_missing_pit_durations_and_stint_compound(spark: SparkSession):
    stints = spark.createDataFrame(
        [{"session_key": 9472, "driver_number": 1, "stint_number": 1, "lap_start": 1, "lap_end": 10}]
    )
    pit = spark.createDataFrame(
        [{"session_key": 9472, "driver_number": 1, "lap_number": 10}]
    )
    drivers = spark.createDataFrame([{"session_key": 9472, "driver_number": 1}])

    row = compute_pit_strategy(stints, pit, drivers).first()

    assert row["compound"] == "UNKNOWN"
    assert row["pit_lap"] == 10
    assert row["pit_duration"] is None
    assert row["driver_name"] == ""


def test_weather_impact_defaults_optional_weather_columns(spark: SparkSession):
    weather = spark.createDataFrame([{"session_key": 9472}])
    laps = spark.createDataFrame(
        [{"session_key": 9472, "lap_time_seconds": 95.0, "lap_number": 1}]
    )

    row = compute_weather_impact(weather, laps).first()

    assert row["field_avg_lap_time"] == 95.0
    assert row["air_temperature"] is None
    assert row["date_utc"] is None


def test_gold_aggregations_accept_empty_inputs(spark: SparkSession):
    empty_laps = spark.createDataFrame(
        [], "session_key int, driver_number int, lap_number int, lap_time_seconds double"
    )
    empty_drivers = spark.createDataFrame([], "session_key int, driver_number int")
    empty_stints = spark.createDataFrame(
        [],
        "session_key int, driver_number int, stint_number int, lap_start int, lap_end int, tyre_life int, tyre_compound string",
    )
    empty_pit = spark.createDataFrame(
        [], "session_key int, driver_number int, lap_number int"
    )
    empty_weather = spark.createDataFrame([], "session_key int")

    assert compute_driver_race_summary(empty_laps, empty_drivers, empty_stints, empty_pit).count() == 0
    assert compute_tyre_degradation(empty_laps, empty_drivers, empty_stints).count() == 0
    assert compute_pit_strategy(empty_stints, empty_pit, empty_drivers).count() == 0
    assert compute_weather_impact(empty_weather, empty_laps).count() == 0
