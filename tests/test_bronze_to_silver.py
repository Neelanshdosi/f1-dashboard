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


def test_transform_fct_session_result_types_and_variations(spark: SparkSession):
    """Test session_result transformation across all gap/duration variations:
    float, "+1 LAP", "+2 LAPS", array string, null, and missing field.
    """
    from spark_jobs.bronze_to_silver import transform_fct_session_result

    raw = [
        # 1. Leader: float duration, numeric 0 gap
        {
            "session_key": 9662,
            "meeting_key": 1252,
            "driver_number": 4,
            "position": 1,
            "number_of_laps": 58,
            "points": 25.0,
            "dnf": False,
            "dns": False,
            "dsq": False,
            "duration": "5193.291",
            "gap_to_leader": "0",
        },
        # 2. P2 finisher: float duration, float gap
        {
            "session_key": 9662,
            "meeting_key": 1252,
            "driver_number": 55,
            "position": 2,
            "number_of_laps": 58,
            "points": 18.0,
            "dnf": False,
            "dns": False,
            "dsq": False,
            "duration": "5199.123",
            "gap_to_leader": "5.832",
        },
        # 3. Lapped driver (+1 LAP)
        {
            "session_key": 9662,
            "meeting_key": 1252,
            "driver_number": 23,
            "position": 11,
            "number_of_laps": 57,
            "points": 0.0,
            "dnf": False,
            "dns": False,
            "dsq": False,
            "duration": None,
            "gap_to_leader": "+1 LAP",
        },
        # 4. Multi-lapped driver (+2 LAPS)
        {
            "session_key": 9662,
            "meeting_key": 1252,
            "driver_number": 22,
            "position": 12,
            "number_of_laps": 56,
            "points": 0.0,
            "dnf": False,
            "dns": False,
            "dsq": False,
            "duration": None,
            "gap_to_leader": "+2 LAPS",
        },
        # 5. DNF driver: null position, null duration & gap
        {
            "session_key": 9662,
            "meeting_key": 1252,
            "driver_number": 77,
            "position": None,
            "number_of_laps": 30,
            "points": 0.0,
            "dnf": True,
            "dns": False,
            "dsq": False,
            "duration": None,
            "gap_to_leader": None,
        },
        # 6. Array string (e.g. from qualifying/practice data)
        {
            "session_key": 7783,
            "meeting_key": 1143,
            "driver_number": 1,
            "position": 1,
            "number_of_laps": 24,
            "points": None,
            "dnf": False,
            "dns": False,
            "dsq": False,
            "duration": "[77.384, 77.056, 76.732]",
            "gap_to_leader": "[0.0, 0.0, 0.0]",
        },
        # 7. Missing fields (dnf/dns/dsq missing, duration/gap missing, duplicate driver)
        {
            "session_key": 9662,
            "meeting_key": 1252,
            "driver_number": 4,  # Duplicate of driver 4, must be deduped
            "position": 1,
        },
    ]
    df = spark.createDataFrame(raw)
    res = transform_fct_session_result(df)

    # Deduped count should be 6
    assert res.count() == 6
    rows_by_driver = {r["driver_number"]: r for r in res.collect()}

    # Check 1: Leader
    r1 = rows_by_driver[4]
    assert r1["position"] == 1
    assert r1["duration_seconds"] == 5193.291
    assert r1["gap_to_leader_seconds"] == 0.0
    assert r1["laps_down"] is None

    # Check 2: P2
    r2 = rows_by_driver[55]
    assert r2["position"] == 2
    assert r2["gap_to_leader_seconds"] == 5.832
    assert r2["laps_down"] is None

    # Check 3: +1 LAP
    r3 = rows_by_driver[23]
    assert r3["laps_down"] == 1
    assert r3["gap_to_leader_seconds"] is None
    assert r3["gap_to_leader_raw"] == "+1 LAP"

    # Check 4: +2 LAPS
    r4 = rows_by_driver[22]
    assert r4["laps_down"] == 2
    assert r4["gap_to_leader_seconds"] is None

    # Check 5: DNF with null position
    r5 = rows_by_driver[77]
    assert r5["position"] is None
    assert r5["dnf"] is True
    assert r5["duration_seconds"] is None
    assert r5["gap_to_leader_seconds"] is None

    # Check 6: Array string
    r6 = rows_by_driver[1]
    assert r6["duration_seconds"] is None
    assert r6["gap_to_leader_seconds"] is None
    assert r6["gap_to_leader_raw"] == "[0.0, 0.0, 0.0]"


def test_transform_fct_starting_grid(spark: SparkSession):
    """Test starting grid transformation with null position and deduplication."""
    from spark_jobs.bronze_to_silver import transform_fct_starting_grid

    raw = [
        {"session_key": 7783, "meeting_key": 1143, "driver_number": 1, "position": 1, "lap_duration": 76.732},
        {"session_key": 7783, "meeting_key": 1143, "driver_number": 2, "position": None, "lap_duration": None},
        # Duplicate
        {"session_key": 7783, "meeting_key": 1143, "driver_number": 1, "position": 1, "lap_duration": 76.732},
    ]
    df = spark.createDataFrame(raw)
    res = transform_fct_starting_grid(df)

    assert res.count() == 2
    rows_by_driver = {r["driver_number"]: r for r in res.collect()}
    assert rows_by_driver[1]["position"] == 1
    assert rows_by_driver[1]["lap_duration"] == 76.732
    assert rows_by_driver[2]["position"] is None
    assert rows_by_driver[2]["lap_duration"] is None


def test_transform_fct_session_result_and_grid_from_real_fixtures(spark: SparkSession):
    """Item 3.a test: Real-file Silver test reading tests/fixtures/session_result_9662.json,
    session_result_7783.json, starting_grid_9662.json, and starting_grid_7783.json through
    the explicit StructType read path, followed by transformation assertions:
    - P1 has points and duration_seconds
    - A lapped finisher has laps_down > 0 and gap_to_leader_seconds is null
    - A DNF has null position and dnf is true
    - Array-valued duration/gap cases (from 7783) yield null numeric fields with raw string preserved
    - Starting grid returns proper positions and lap_durations
    """
    from pathlib import Path

    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    from spark_jobs.bronze_to_silver import (
        transform_fct_session_result,
        transform_fct_starting_grid,
    )

    fixtures_dir = Path(__file__).parent / "fixtures"

    res_schema = StructType([
        StructField("session_key", IntegerType(), True),
        StructField("meeting_key", IntegerType(), True),
        StructField("driver_number", IntegerType(), True),
        StructField("position", IntegerType(), True),
        StructField("number_of_laps", IntegerType(), True),
        StructField("points", DoubleType(), True),
        StructField("dnf", BooleanType(), True),
        StructField("dns", BooleanType(), True),
        StructField("dsq", BooleanType(), True),
        StructField("duration", StringType(), True),
        StructField("gap_to_leader", StringType(), True),
    ])

    grid_schema = StructType([
        StructField("session_key", IntegerType(), True),
        StructField("meeting_key", IntegerType(), True),
        StructField("driver_number", IntegerType(), True),
        StructField("position", IntegerType(), True),
        StructField("lap_duration", DoubleType(), True),
    ])

    # 1. Test 9662 (Race results)
    raw_res_9662 = spark.read.schema(res_schema).option("multiline", "true").json(
        str(fixtures_dir / "session_result_9662.json")
    )
    silver_res_9662 = transform_fct_session_result(raw_res_9662)
    assert silver_res_9662.count() == 20
    rows_9662 = {r["driver_number"]: r for r in silver_res_9662.collect()}

    # Assertion 1: P1 has points and duration_seconds
    p1 = rows_9662[4]
    assert p1["position"] == 1
    assert p1["points"] == 25.0
    assert p1["duration_seconds"] == 5193.291
    assert p1["gap_to_leader_seconds"] == 0.0

    # Assertion 2: Lapped finisher has laps_down > 0 and gap_to_leader_seconds null
    lapped = rows_9662[23]
    assert lapped["laps_down"] == 1
    assert lapped["gap_to_leader_seconds"] is None
    assert lapped["gap_to_leader_raw"] == "+1 LAP"

    # Assertion 3: DNF has null position and dnf true
    dnf_driver = rows_9662[77]
    assert dnf_driver["position"] is None
    assert dnf_driver["dnf"] is True
    assert dnf_driver["duration_seconds"] is None
    assert dnf_driver["gap_to_leader_seconds"] is None

    # 2. Test 7783 (Array duration/gap case)
    raw_res_7783 = spark.read.schema(res_schema).option("multiline", "true").json(
        str(fixtures_dir / "session_result_7783.json")
    )
    silver_res_7783 = transform_fct_session_result(raw_res_7783)
    assert silver_res_7783.count() == 20
    rows_7783 = {r["driver_number"]: r for r in silver_res_7783.collect()}

    # Assertion 4: Array-valued duration/gap yields null numeric fields, raw preserved
    arr_driver = rows_7783[1]
    assert arr_driver["duration_seconds"] is None
    assert arr_driver["gap_to_leader_seconds"] is None
    assert arr_driver["gap_to_leader_raw"] is not None
    assert arr_driver["gap_to_leader_raw"].startswith("[")

    # 3. Test 7783 Starting Grid
    raw_grid_7783 = spark.read.schema(grid_schema).option("multiline", "true").json(
        str(fixtures_dir / "starting_grid_7783.json")
    )
    silver_grid_7783 = transform_fct_starting_grid(raw_grid_7783)
    assert silver_grid_7783.count() == 18
    grid_rows_7783 = {r["driver_number"]: r for r in silver_grid_7783.collect()}
    assert grid_rows_7783[1]["position"] == 1
    assert grid_rows_7783[1]["lap_duration"] == 76.732


def test_bronze_to_silver_pipeline_tolerates_missing_starting_grid(tmp_path):
    """Plan Change A test: Bronze-to-Silver completes cleanly when starting_grid Bronze folder does not exist."""
    import json

    from spark_jobs.bronze_to_silver import run_pipeline

    base_dir = tmp_path / "lakehouse"
    # Create minimal bronze files for session 9472
    sess_dir = base_dir / "bronze" / "sessions" / "season=2024" / "all_2024"
    sess_dir.mkdir(parents=True)
    with (sess_dir / "sessions.json").open("w") as f:
        json.dump([
            {
                "session_key": 9472,
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
                "is_cancelled": False,
            }
        ], f)

    d_dir = base_dir / "bronze" / "drivers" / "season=2024" / "session_key=9472"
    d_dir.mkdir(parents=True)
    with (d_dir / "drivers.json").open("w") as f:
        json.dump([
            {
                "session_key": 9472,
                "meeting_key": 1229,
                "driver_number": 1,
                "full_name": "Max Verstappen",
                "name_acronym": "VER",
                "team_name": "Red Bull Racing",
                "team_colour": "3671C6",
            }
        ], f)

    # Note: starting_grid and session_result bronze folders are intentionally NOT created
    base_uri = base_dir.as_uri()
    counts = run_pipeline(session_key=9472, season=2024, base_uri=base_uri)

    assert "dim_sessions" in counts
    assert "dim_drivers" in counts
    assert "fct_starting_grid" not in counts

    # Check audit log contains starting_grid with rows_in=0
    audit_file = base_dir / "pipeline_run_audit.jsonl"
    assert audit_file.exists()
    with audit_file.open("r", encoding="utf-8") as f:
        audit_records = [json.loads(line) for line in f if line.strip()]

    sg_audits = [r for r in audit_records if r["table_name"] == "fct_starting_grid"]
    assert len(sg_audits) == 1
    assert sg_audits[0]["rows_in"] == 0
    assert sg_audits[0]["rows_out"] == 0
    assert sg_audits[0]["status"] == "NO_DATA"
