"""
Unit tests for PySpark Bronze to Silver transformations.
"""

import pytest
from pyspark.sql import SparkSession

from spark_jobs.bronze_to_silver import (
    get_spark_session,
    transform_dim_drivers,
    transform_dim_sessions,
    transform_fct_laps,
    transform_fct_pit,
    transform_fct_stints,
    transform_fct_weather,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return get_spark_session("test_f1_transforms")


def test_transform_dim_sessions(spark: SparkSession):
    raw_data = [
        {
            "session_key": 9472,
            "meeting_key": 1229,
            "circuit_key": 63,
            "circuit_short_name": "Sakhir ",
            "country_name": "Bahrain ",
            "country_code": "BRN",
            "location": "Sakhir",
            "session_type": "Race",
            "session_name": "Race",
            "date_start": "2024-03-02T15:00:00+00:00",
            "date_end": "2024-03-02T17:00:00+00:00",
            "gmt_offset": "03:00:00",
            "year": 2024,
            "is_cancelled": False,
        },
        # Cancelled session should be excluded
        {
            "session_key": 9999,
            "meeting_key": 1229,
            "circuit_key": 63,
            "circuit_short_name": "Sakhir",
            "country_name": "Bahrain",
            "country_code": "BRN",
            "location": "Sakhir",
            "session_type": "Race",
            "session_name": "Race",
            "date_start": "2024-03-02T15:00:00+00:00",
            "date_end": "2024-03-02T17:00:00+00:00",
            "gmt_offset": "03:00:00",
            "year": 2024,
            "is_cancelled": True,
        },
    ]
    df = spark.createDataFrame(raw_data)
    result = transform_dim_sessions(df)

    assert result.count() == 1
    row = result.first()
    assert row["session_key"] == 9472
    assert row["circuit_short_name"] == "Sakhir"
    assert row["season"] == 2024


def test_transform_dim_drivers(spark: SparkSession):
    raw_data = [
        {
            "meeting_key": 1229,
            "session_key": 9472,
            "driver_number": 1,
            "broadcast_name": "M VERSTAPPEN ",
            "full_name": "Max VERSTAPPEN",
            "first_name": "Max",
            "last_name": "Verstappen",
            "name_acronym": "VER",
            "team_name": "Red Bull Racing",
            "team_colour": "3671c6",
            "headshot_url": "http://img.png",
            "country_code": "NED",
        },
        # Duplicate should be removed
        {
            "meeting_key": 1229,
            "session_key": 9472,
            "driver_number": 1,
            "broadcast_name": "M VERSTAPPEN",
            "full_name": "Max VERSTAPPEN",
            "first_name": "Max",
            "last_name": "Verstappen",
            "name_acronym": "VER",
            "team_name": "Red Bull Racing",
            "team_colour": "3671c6",
            "headshot_url": "http://img.png",
            "country_code": "NED",
        },
    ]
    df = spark.createDataFrame(raw_data)
    result = transform_dim_drivers(df)

    assert result.count() == 1
    row = result.first()
    assert row["broadcast_name"] == "M VERSTAPPEN"
    assert row["name_acronym"] == "VER"


def test_transform_fct_laps_with_stints(spark: SparkSession):
    raw_stints = [
        {
            "meeting_key": 1229,
            "session_key": 9472,
            "stint_number": 1,
            "driver_number": 1,
            "lap_start": 1,
            "lap_end": 17,
            "compound": "soft",
            "tyre_age_at_start": 3,
        }
    ]
    raw_laps = [
        {
            "meeting_key": 1229,
            "session_key": 9472,
            "driver_number": 1,
            "lap_number": 5,
            "date_start": "2024-03-02T15:03:42.341000+00:00",
            "duration_sector_1": 32.877,
            "duration_sector_2": 41.266,
            "duration_sector_3": 23.616,
            "i1_speed": 234.0,
            "i2_speed": 250.0,
            "is_pit_out_lap": False,
            "lap_duration": 97.759,
            "st_speed": 251.0,
        }
    ]
    df_stints = transform_fct_stints(spark.createDataFrame(raw_stints))
    df_laps = spark.createDataFrame(raw_laps)
    result = transform_fct_laps(df_laps, df_stints=df_stints)

    assert result.count() == 1
    row = result.first()
    assert row["lap_number"] == 5
    assert row["tyre_compound"] == "SOFT"
    # tyre_life = lap_number (5) - lap_start (1) + tyre_age_at_start (3) = 7
    assert row["tyre_life"] == 7
    assert row["lap_time_seconds"] == 97.759
