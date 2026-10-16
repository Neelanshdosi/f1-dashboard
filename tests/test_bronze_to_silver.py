"""
Unit tests for PySpark Bronze to Silver transformations.
"""

from pyspark.sql import SparkSession

from spark_jobs.bronze_to_silver import (
    transform_dim_drivers,
    transform_dim_sessions,
    transform_fct_laps,
    transform_fct_pit,
    transform_fct_stints,
    transform_fct_weather,
)


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
            "lap_duration": "97.759",
            "st_speed": 251.0,
        },
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
            "lap_duration": "97.759",
            "st_speed": 251.0,
        },
        {
            "meeting_key": 1229,
            "session_key": 9472,
            "driver_number": 1,
            "lap_number": 6,
            "date_start": "2024-03-02T15:05:00+00:00",
            "duration_sector_1": 33.0,
            "duration_sector_2": 41.0,
            "duration_sector_3": 24.0,
            "i1_speed": 234.0,
            "i2_speed": 250.0,
            "is_pit_out_lap": True,
            "lap_duration": "invalid",
            "st_speed": 251.0,
        },
    ]
    df_stints = transform_fct_stints(spark.createDataFrame(raw_stints))
    df_laps = spark.createDataFrame(raw_laps)
    result = transform_fct_laps(df_laps, df_stints=df_stints)

    assert result.count() == 2
    row = result.filter("lap_number = 5").first()
    assert row["lap_number"] == 5
    assert row["tyre_compound"] == "SOFT"
    # tyre_life = lap_number (5) - lap_start (1) + tyre_age_at_start (3) = 7
    assert row["tyre_life"] == 7
    assert row["lap_time_seconds"] == 97.759
    out_lap = result.filter("lap_number = 6").first()
    assert out_lap["is_pit_out_lap"] is True
    assert out_lap["lap_time_seconds"] is None


def test_transform_fct_stints_deduplicates_and_defaults_optional_fields(spark: SparkSession):
    raw = [
        {
            "session_key": 9472,
            "driver_number": 1,
            "stint_number": 1,
            "lap_start": 1,
            "lap_end": 10,
        },
        {
            "session_key": 9472,
            "driver_number": 1,
            "stint_number": 1,
            "lap_start": 1,
            "lap_end": 10,
        },
    ]
    result = transform_fct_stints(spark.createDataFrame(raw))

    assert result.count() == 1
    row = result.first()
    assert row["compound"] == "UNKNOWN"
    assert row["tyre_age_at_start"] == 0
    assert row["meeting_key"] is None


def test_transform_fct_laps_defaults_missing_optional_columns(spark: SparkSession):
    raw = [
        {
            "session_key": 9472,
            "driver_number": 1,
            "lap_number": 1,
            "lap_duration": 95.0,
        }
    ]
    result = transform_fct_laps(spark.createDataFrame(raw))
    row = result.first()

    assert row["is_pit_out_lap"] is False
    assert row["lap_time_seconds"] == 95.0
    assert row["meeting_key"] is None


def test_transform_fct_pit_deduplicates_and_defaults_optional_fields(spark: SparkSession):
    raw = [
        {"session_key": 9472, "driver_number": 1, "lap_number": 10},
        {"session_key": 9472, "driver_number": 1, "lap_number": 10},
    ]
    result = transform_fct_pit(spark.createDataFrame(raw))

    assert result.count() == 1
    row = result.first()
    assert row["pit_duration"] is None
    assert row["date_start"] is None


def test_transform_fct_weather_deduplicates_and_defaults_optional_fields(spark: SparkSession):
    raw = [
        {"session_key": 9472, "date": "2024-03-02T15:00:00+00:00"},
        {"session_key": 9472, "date": "2024-03-02T15:00:00+00:00"},
    ]
    result = transform_fct_weather(spark.createDataFrame(raw))

    assert result.count() == 1
    assert result.first()["air_temperature"] is None


def test_transform_dim_drivers_defaults_missing_optional_columns(spark: SparkSession):
    raw = [
        {"session_key": 9472, "driver_number": 1},
        {"session_key": 9472, "driver_number": 1},
    ]
    result = transform_dim_drivers(spark.createDataFrame(raw))

    assert result.count() == 1
    row = result.first()
    assert row["full_name"] == ""
    assert row["country_code"] == ""
