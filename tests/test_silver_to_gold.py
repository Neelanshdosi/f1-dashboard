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


def test_compute_race_results_positions_gained_and_status_rules(spark: SparkSession):
    """Test compute_race_results with:
    - positions_gained: positive (gained), negative (lost), null (null grid or null final)
    - status rule check order: dsq -> "DSQ", dns -> "DNS", dnf -> "DNF", position not null -> "Finished", otherwise -> "Unclassified"
    - multiple flags set (e.g. dsq + dnf -> DSQ takes precedence)
    - missing from result table -> "Unclassified"
    - pit stops count joining
    """
    from spark_jobs.silver_to_gold import compute_race_results

    drivers_data = [
        {"session_key": 9472, "driver_number": 1, "full_name": "Max Verstappen", "name_acronym": "VER", "team_name": "Red Bull Racing", "team_colour": "3671c6"},
        {"session_key": 9472, "driver_number": 44, "full_name": "Lewis Hamilton", "name_acronym": "HAM", "team_name": "Mercedes", "team_colour": "6cd3bf"},
        {"session_key": 9472, "driver_number": 16, "full_name": "Charles Leclerc", "name_acronym": "LEC", "team_name": "Ferrari", "team_colour": "e8002d"},
        {"session_key": 9472, "driver_number": 77, "full_name": "Valtteri Bottas", "name_acronym": "BOT", "team_name": "Kick Sauber", "team_colour": "52e252"},
        {"session_key": 9472, "driver_number": 20, "full_name": "Kevin Magnussen", "name_acronym": "MAG", "team_name": "Haas", "team_colour": "b6babd"},
        {"session_key": 9472, "driver_number": 31, "full_name": "Esteban Ocon", "name_acronym": "OCO", "team_name": "Alpine", "team_colour": "0093cc"},
        {"session_key": 9472, "driver_number": 99, "full_name": "Reserve Driver", "name_acronym": "RES", "team_name": "Reserve Team", "team_colour": "ffffff"},
    ]

    result_data = [
        # Driver 1: Started P2, Finished P1 -> positions_gained = +1
        {"session_key": 9472, "driver_number": 1, "position": 1, "number_of_laps": 57, "points": 25.0, "dnf": False, "dns": False, "dsq": False, "duration_seconds": 5400.0, "gap_to_leader_seconds": 0.0, "laps_down": None},
        # Driver 44: Started P1, Finished P4 -> positions_gained = -3
        {"session_key": 9472, "driver_number": 44, "position": 4, "number_of_laps": 57, "points": 12.0, "dnf": False, "dns": False, "dsq": False, "duration_seconds": 5420.0, "gap_to_leader_seconds": 20.0, "laps_down": None},
        # Driver 16: DNF with null position -> positions_gained = null, status = "DNF"
        {"session_key": 9472, "driver_number": 16, "position": None, "number_of_laps": 20, "points": 0.0, "dnf": True, "dns": False, "dsq": False, "duration_seconds": None, "gap_to_leader_seconds": None, "laps_down": None},
        # Driver 77: DNS -> status = "DNS"
        {"session_key": 9472, "driver_number": 77, "position": None, "number_of_laps": 0, "points": 0.0, "dnf": False, "dns": True, "dsq": False, "duration_seconds": None, "gap_to_leader_seconds": None, "laps_down": None},
        # Driver 20: DSQ + DNF both True -> status = "DSQ" (precedence)
        {"session_key": 9472, "driver_number": 20, "position": None, "number_of_laps": 10, "points": 0.0, "dnf": True, "dns": False, "dsq": True, "duration_seconds": None, "gap_to_leader_seconds": None, "laps_down": None},
        # Driver 31: Pit-lane starter (grid position null), Finished P10 -> positions_gained = null
        {"session_key": 9472, "driver_number": 31, "position": 10, "number_of_laps": 57, "points": 1.0, "dnf": False, "dns": False, "dsq": False, "duration_seconds": 5450.0, "gap_to_leader_seconds": 50.0, "laps_down": None},
        # Driver 99 is intentionally missing from result_data
    ]

    grid_data = [
        {"session_key": 9472, "driver_number": 1, "position": 2},
        {"session_key": 9472, "driver_number": 44, "position": 1},
        {"session_key": 9472, "driver_number": 16, "position": 3},
        {"session_key": 9472, "driver_number": 77, "position": 15},
        {"session_key": 9472, "driver_number": 20, "position": 18},
        {"session_key": 9472, "driver_number": 31, "position": None},  # Pit-lane starter
        {"session_key": 9472, "driver_number": 99, "position": 20},
    ]

    pit_data = [
        {"session_key": 9472, "driver_number": 1},
        {"session_key": 9472, "driver_number": 1},  # 2 pit stops
        {"session_key": 9472, "driver_number": 44},  # 1 pit stop
    ]

    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        StructField,
        StructType,
    )

    result_schema = StructType([
        StructField("session_key", IntegerType(), True),
        StructField("driver_number", IntegerType(), True),
        StructField("position", IntegerType(), True),
        StructField("number_of_laps", IntegerType(), True),
        StructField("points", DoubleType(), True),
        StructField("dnf", BooleanType(), True),
        StructField("dns", BooleanType(), True),
        StructField("dsq", BooleanType(), True),
        StructField("duration_seconds", DoubleType(), True),
        StructField("gap_to_leader_seconds", DoubleType(), True),
        StructField("laps_down", IntegerType(), True),
    ])

    grid_schema = StructType([
        StructField("session_key", IntegerType(), True),
        StructField("driver_number", IntegerType(), True),
        StructField("position", IntegerType(), True),
    ])

    df_drivers = spark.createDataFrame(drivers_data)
    df_result = spark.createDataFrame(result_data, schema=result_schema)
    df_grid = spark.createDataFrame(grid_data, schema=grid_schema)
    df_pit = spark.createDataFrame(pit_data)

    res = compute_race_results(df_result, df_grid, df_drivers, df_pit=df_pit)
    assert res.count() == 7
    rows = {r["driver_number"]: r for r in res.collect()}

    # Driver 1: +1 position gained, Finished, 2 pit stops
    assert rows[1]["grid_position"] == 2
    assert rows[1]["final_position"] == 1
    assert rows[1]["positions_gained"] == 1
    assert rows[1]["status"] == "Finished"
    assert rows[1]["total_pit_stops"] == 2
    assert rows[1]["grid_position_missing"] is False

    # Driver 44: -3 positions gained (lost 3), Finished, 1 pit stop
    assert rows[44]["grid_position"] == 1
    assert rows[44]["final_position"] == 4
    assert rows[44]["positions_gained"] == -3
    assert rows[44]["status"] == "Finished"
    assert rows[44]["total_pit_stops"] == 1

    # Driver 16: DNF with null final position -> null positions_gained
    assert rows[16]["status"] == "DNF"
    assert rows[16]["final_position"] is None
    assert rows[16]["positions_gained"] is None

    # Driver 77: DNS -> null positions_gained
    assert rows[77]["status"] == "DNS"
    assert rows[77]["positions_gained"] is None

    # Driver 20: Both DSQ and DNF -> DSQ rule precedence
    assert rows[20]["status"] == "DSQ"

    # Driver 31: Pit-lane starter with null grid -> null positions_gained, grid_position_missing=True
    assert rows[31]["grid_position"] is None
    assert rows[31]["final_position"] == 10
    assert rows[31]["positions_gained"] is None
    assert rows[31]["grid_position_missing"] is True

    # Driver 99: Missing from results -> "Unclassified", 0 pit stops
    assert rows[99]["status"] == "Unclassified"
    assert rows[99]["final_position"] is None
    assert rows[99]["total_pit_stops"] == 0


def test_compute_race_results_when_grid_is_none(spark: SparkSession):
    """Plan Change B: compute_race_results functions cleanly when df_grid is None."""
    from spark_jobs.silver_to_gold import compute_race_results

    drivers_data = [
        {"session_key": 9472, "driver_number": 1, "full_name": "Max Verstappen", "team_name": "Red Bull", "team_colour": "3671c6"},
    ]
    result_data = [
        {"session_key": 9472, "driver_number": 1, "position": 1, "number_of_laps": 57, "points": 25.0, "dnf": False, "dns": False, "dsq": False},
    ]

    df_drivers = spark.createDataFrame(drivers_data)
    df_result = spark.createDataFrame(result_data)

    res = compute_race_results(df_result=df_result, df_grid=None, df_drivers=df_drivers)
    assert res.count() == 1
    r = res.first()
    assert r["final_position"] == 1
    assert r["grid_position"] is None
    assert r["positions_gained"] is None
    assert r["grid_position_missing"] is True
    assert r["status"] == "Finished"


def test_run_pipeline_no_data_audit_when_session_result_missing(spark: SparkSession, tmp_path, monkeypatch):
    """NO-RESULTS RULE: if Silver fct_session_result is missing or empty for a session,

    write NO rows to agg_race_results for it, log an error, and record status NO_DATA
    in the audit table.
    """
    import json

    from spark_jobs.silver_to_gold import run_pipeline

    session_key = 9999
    season = 2024
    base_uri = f"file:///{tmp_path.as_posix()}"

    # Create dummy Silver dim_drivers and fct_laps so pipeline runs
    drivers_dir = tmp_path / "silver" / "dim_drivers" / f"season={season}" / f"session_key={session_key}"
    drivers_dir.mkdir(parents=True, exist_ok=True)
    df_drivers = spark.createDataFrame([{"session_key": session_key, "driver_number": 1, "full_name": "Test Driver", "team_name": "Test", "team_colour": "fff"}])
    df_drivers.write.mode("overwrite").parquet(str(drivers_dir))

    # Note: no fct_session_result folder created at all

    # Run pipeline
    counts = run_pipeline(session_key=session_key, season=season, base_uri=base_uri)
    assert "agg_race_results" not in counts

    # Check gold output directory does not exist or has no parquet files
    gold_dir = tmp_path / "gold" / "agg_race_results" / f"season={season}" / f"session_key={session_key}"
    assert not gold_dir.exists()

    # Read audit log
    audit_file = tmp_path / "pipeline_run_audit.jsonl"
    assert audit_file.exists()
    audit_records = [json.loads(line) for line in audit_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    agg_res_audits = [r for r in audit_records if r["table_name"] == "agg_race_results"]
    assert len(agg_res_audits) == 1
    audit = agg_res_audits[0]
    assert audit["status"] == "NO_DATA"
    assert audit["rows_in"] == 0
    assert audit["rows_out"] == 0


