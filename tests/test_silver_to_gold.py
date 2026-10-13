"""
Unit tests for PySpark Silver to Gold aggregations.
"""

import pytest
from pyspark.sql import SparkSession

from spark_jobs.bronze_to_silver import get_spark_session
from spark_jobs.silver_to_gold import (
    compute_driver_race_summary,
    compute_pit_strategy,
    compute_tyre_degradation,
    compute_weather_impact,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return get_spark_session("test_gold_transforms")


def test_compute_driver_race_summary(spark: SparkSession):
    laps_data = [
        {
            "session_key": 9472,
            "meeting_key": 1229,
            "driver_number": 1,
            "lap_number": 1,
            "lap_time_seconds": 98.5,
            "is_pit_out_lap": False,
        },
        {
            "session_key": 9472,
            "meeting_key": 1229,
            "driver_number": 1,
            "lap_number": 2,
            "lap_time_seconds": 95.0,
            "is_pit_out_lap": False,
        },
    ]
    drivers_data = [
        {
            "session_key": 9472,
            "driver_number": 1,
            "full_name": "Max Verstappen",
            "name_acronym": "VER",
            "team_name": "Red Bull Racing",
            "team_colour": "3671c6",
        }
    ]
    stints_data = [
        {
            "session_key": 9472,
            "driver_number": 1,
            "stint_number": 1,
            "compound": "SOFT",
            "lap_start": 1,
            "lap_end": 2,
        }
    ]
    pit_data = [
        {
            "session_key": 9472,
            "driver_number": 1,
            "lap_number": 2,
            "pit_duration": 24.5,
        }
    ]

    df_laps = spark.createDataFrame(laps_data)
    df_drivers = spark.createDataFrame(drivers_data)
    df_stints = spark.createDataFrame(stints_data)
    df_pit = spark.createDataFrame(pit_data)

    res = compute_driver_race_summary(df_laps, df_drivers, df_stints, df_pit)
    assert res.count() == 1
    row = res.first()
    assert row["driver_name"] == "Max Verstappen"
    assert row["fastest_lap_seconds"] == 95.0
    assert row["total_laps"] == 2
    assert row["total_pit_stops"] == 1
    assert row["total_pit_duration"] == 24.5
    assert row["starting_compound"] == "SOFT"


def test_compute_tyre_degradation(spark: SparkSession):
    laps_data = [
        {
            "session_key": 9472,
            "driver_number": 1,
            "stint_number": 1,
            "tyre_compound": "SOFT",
            "tyre_life": 5,
            "lap_number": 5,
            "lap_time_seconds": 96.0,
            "is_pit_out_lap": False,
        },
        {
            "session_key": 9472,
            "driver_number": 1,
            "stint_number": 1,
            "tyre_compound": "SOFT",
            "tyre_life": 6,
            "lap_number": 6,
            "lap_time_seconds": 98.0,
            "is_pit_out_lap": False,
        },
    ]
    drivers_data = [
        {
            "session_key": 9472,
            "driver_number": 1,
            "full_name": "Max Verstappen",
            "team_name": "Red Bull Racing",
        }
    ]
    stints_data = [
        {
            "session_key": 9472,
            "driver_number": 1,
            "stint_number": 1,
            "compound": "SOFT",
            "lap_start": 1,
            "lap_end": 10,
        }
    ]
    df_laps = spark.createDataFrame(laps_data)
    df_drivers = spark.createDataFrame(drivers_data)
    df_stints = spark.createDataFrame(stints_data)

    res = compute_tyre_degradation(df_laps, df_drivers, df_stints)
    assert res.count() == 2
    rows = {r["lap_number"]: r for r in res.collect()}
    assert rows[5]["stint_avg_lap_time"] == 97.0
    assert rows[5]["delta_to_stint_avg"] == -1.0
    assert rows[6]["delta_to_stint_avg"] == 1.0
