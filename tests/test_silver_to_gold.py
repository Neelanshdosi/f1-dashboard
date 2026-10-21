"""
Unit tests for PySpark Silver to Gold aggregations.
"""

from pyspark.sql import SparkSession

from spark_jobs.silver_to_gold import (
    compute_driver_race_summary,
    compute_driver_telemetry,
    compute_tyre_degradation,
)


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


def test_compute_driver_telemetry(spark: SparkSession):
    car_data = [
        # Driver 1: 4 records
        {"session_key": 9472, "meeting_key": 1229, "driver_number": 1, "speed": 300, "throttle": 100, "brake": 0, "drs": 12},
        {"session_key": 9472, "meeting_key": 1229, "driver_number": 1, "speed": 320, "throttle": 100, "brake": 0, "drs": 14},
        {"session_key": 9472, "meeting_key": 1229, "driver_number": 1, "speed": 150, "throttle": 50, "brake": 100, "drs": 1},
        {"session_key": 9472, "meeting_key": 1229, "driver_number": 1, "speed": 230, "throttle": 0, "brake": 50, "drs": 8},
        # Driver 44: 2 records
        {"session_key": 9472, "meeting_key": 1229, "driver_number": 44, "speed": 200, "throttle": 80, "brake": 0, "drs": 0},
        {"session_key": 9472, "meeting_key": 1229, "driver_number": 44, "speed": 250, "throttle": 100, "brake": 0, "drs": 10},
    ]
    drivers_data = [
        {"session_key": 9472, "driver_number": 1, "full_name": "Max Verstappen", "team_name": "Red Bull Racing", "team_colour": "3671c6"},
        {"session_key": 9472, "driver_number": 44, "full_name": "Lewis Hamilton", "team_name": "Mercedes", "team_colour": "6cd3bf"},
    ]
    df_car_data = spark.createDataFrame(car_data)
    df_drivers = spark.createDataFrame(drivers_data)

    res = compute_driver_telemetry(df_car_data, df_drivers)
    assert res.count() == 2
    rows = {r["driver_number"]: r for r in res.collect()}

    d1 = rows[1]
    assert d1["driver_name"] == "Max Verstappen"
    assert d1["team_name"] == "Red Bull Racing"
    assert d1["team_colour"] == "3671c6"
    assert d1["max_speed"] == 320
    assert d1["avg_speed"] == 250.0
    assert d1["avg_throttle"] == 62.5
    assert d1["full_throttle_pct"] == 50.0
    assert d1["brake_pct"] == 50.0
    assert d1["drs_open_pct"] == 50.0

    d44 = rows[44]
    assert d44["driver_name"] == "Lewis Hamilton"
    assert d44["team_name"] == "Mercedes"
    assert d44["team_colour"] == "6cd3bf"
    assert d44["max_speed"] == 250
    assert d44["avg_speed"] == 225.0
    assert d44["avg_throttle"] == 90.0
    assert d44["full_throttle_pct"] == 50.0
    assert d44["brake_pct"] == 0.0
    assert d44["drs_open_pct"] == 50.0

