"""
Integration tests for local-mode end-to-end execution of bronze_to_silver and silver_to_gold.
"""

import json
from pathlib import Path

from spark_jobs.bronze_to_silver import run_pipeline as run_bronze_to_silver
from spark_jobs.silver_to_gold import run_pipeline as run_silver_to_gold


def test_end_to_end_local_spark_pipeline(tmp_path: Path):
    session_key = 9472
    season = 2024
    base_dir = tmp_path / "lakehouse"
    base_uri = base_dir.as_uri()  # file:///...

    # Prepare Bronze JSON files
    bronze_dir = base_dir / "bronze"

    # 1. sessions
    sessions_file = bronze_dir / "sessions" / f"season={season}" / f"session_key={session_key}" / "sessions.json"
    sessions_file.parent.mkdir(parents=True, exist_ok=True)
    sessions_file.write_text(
        json.dumps([
            {
                "session_key": session_key,
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
                "year": season,
                "is_cancelled": False,
            }
        ]),
        encoding="utf-8",
    )

    # 2. drivers
    drivers_file = bronze_dir / "drivers" / f"season={season}" / f"session_key={session_key}" / "drivers.json"
    drivers_file.parent.mkdir(parents=True, exist_ok=True)
    drivers_file.write_text(
        json.dumps([
            {
                "session_key": session_key,
                "meeting_key": 1229,
                "driver_number": 1,
                "broadcast_name": "M VERSTAPPEN",
                "full_name": "Max VERSTAPPEN",
                "name_acronym": "VER",
                "team_name": "Red Bull Racing",
                "team_colour": "3671C6",
                "first_name": "Max",
                "last_name": "Verstappen",
                "headshot_url": "https://example.com/ver.png",
                "country_code": "NED",
            }
        ]),
        encoding="utf-8",
    )

    # 3. stints
    stints_file = bronze_dir / "stints" / f"season={season}" / f"session_key={session_key}" / "stints.json"
    stints_file.parent.mkdir(parents=True, exist_ok=True)
    stints_file.write_text(
        json.dumps([
            {
                "session_key": session_key,
                "meeting_key": 1229,
                "driver_number": 1,
                "stint_number": 1,
                "lap_start": 1,
                "lap_end": 10,
                "compound": "SOFT",
                "tyre_age_at_start": 0,
            }
        ]),
        encoding="utf-8",
    )

    # 4. laps
    laps_file = bronze_dir / "laps" / f"season={season}" / f"session_key={session_key}" / "laps.json"
    laps_file.parent.mkdir(parents=True, exist_ok=True)
    laps_file.write_text(
        json.dumps([
            {
                "session_key": session_key,
                "meeting_key": 1229,
                "driver_number": 1,
                "lap_number": 1,
                "lap_duration": 95.5,
                "duration_sector_1": 30.0,
                "duration_sector_2": 35.0,
                "duration_sector_3": 30.5,
                "i1_speed": 280,
                "i2_speed": 290,
                "st_speed": 310,
                "is_pit_out_lap": False,
                "date_start": "2024-03-02T15:01:00+00:00",
            },
            {
                "session_key": session_key,
                "meeting_key": 1229,
                "driver_number": 1,
                "lap_number": 2,
                "lap_duration": 94.2,
                "duration_sector_1": 29.8,
                "duration_sector_2": 34.5,
                "duration_sector_3": 29.9,
                "i1_speed": 285,
                "i2_speed": 295,
                "st_speed": 315,
                "is_pit_out_lap": False,
                "date_start": "2024-03-02T15:02:35+00:00",
            },
        ]),
        encoding="utf-8",
    )

    # 5. pit
    pit_file = bronze_dir / "pit" / f"season={season}" / f"session_key={session_key}" / "pit.json"
    pit_file.parent.mkdir(parents=True, exist_ok=True)
    pit_file.write_text(
        json.dumps([
            {
                "session_key": session_key,
                "meeting_key": 1229,
                "driver_number": 1,
                "lap_number": 10,
                "pit_duration": 22.4,
                "stop_duration": 2.6,
                "date": "2024-03-02T15:20:00+00:00",
            }
        ]),
        encoding="utf-8",
    )

    # 6. weather
    weather_file = bronze_dir / "weather" / f"season={season}" / f"session_key={session_key}" / "weather.json"
    weather_file.parent.mkdir(parents=True, exist_ok=True)
    weather_file.write_text(
        json.dumps([
            {
                "session_key": session_key,
                "meeting_key": 1229,
                "date": "2024-03-02T15:00:00+00:00",
                "air_temperature": 25.0,
                "track_temperature": 32.5,
                "humidity": 45.0,
                "pressure": 1012.0,
                "wind_speed": 2.5,
                "wind_direction": 180,
                "rainfall": 0,
            }
        ]),
        encoding="utf-8",
    )

    # 7. car_data
    car_data_file = bronze_dir / "car_data" / f"season={season}" / f"session_key={session_key}" / "car_data.json"
    car_data_file.parent.mkdir(parents=True, exist_ok=True)
    car_data_file.write_text(
        json.dumps([
            {
                "session_key": session_key,
                "meeting_key": 1229,
                "driver_number": 1,
                "date": "2024-03-02T15:05:00+00:00",
                "rpm": 11000,
                "speed": 315,
                "n_gear": 8,
                "throttle": 100,
                "brake": 0,
                "drs": 12,
            }
        ]),
        encoding="utf-8",
    )

    # Step 1: Run bronze_to_silver with local base-uri
    silver_counts = run_bronze_to_silver(
        session_key=session_key,
        season=season,
        base_uri=base_uri,
    )

    assert silver_counts["dim_sessions"] == 1
    assert silver_counts["dim_drivers"] == 1
    assert silver_counts["fct_stints"] == 1
    assert silver_counts["fct_laps"] == 2
    assert silver_counts["fct_pit"] == 1
    assert silver_counts["fct_weather"] == 1
    assert silver_counts["fct_car_data"] == 1

    silver_dir = base_dir / "silver"
    for tbl in [
        "dim_sessions",
        "dim_drivers",
        "fct_stints",
        "fct_laps",
        "fct_pit",
        "fct_weather",
        "fct_car_data",
    ]:
        tbl_path = silver_dir / tbl / f"season={season}" / f"session_key={session_key}"
        assert tbl_path.exists()
        parquet_files = list(tbl_path.glob("*.parquet"))
        assert len(parquet_files) > 0

    # Step 2: Run silver_to_gold with local base-uri (skips BigQuery)
    gold_counts = run_silver_to_gold(
        session_key=session_key,
        season=season,
        base_uri=base_uri,
    )

    assert gold_counts["agg_driver_race_summary"] == 1
    assert gold_counts["agg_tyre_degradation"] == 2
    assert gold_counts["agg_pit_strategy"] == 1
    assert gold_counts["agg_weather_impact"] == 1
    assert gold_counts["agg_driver_telemetry"] == 1

    gold_dir = base_dir / "gold"
    for tbl in [
        "agg_driver_race_summary",
        "agg_tyre_degradation",
        "agg_pit_strategy",
        "agg_weather_impact",
        "agg_driver_telemetry",
    ]:
        tbl_path = gold_dir / tbl / f"season={season}" / f"session_key={session_key}"
        assert tbl_path.exists()
        parquet_files = list(tbl_path.glob("*.parquet"))
        assert len(parquet_files) > 0
