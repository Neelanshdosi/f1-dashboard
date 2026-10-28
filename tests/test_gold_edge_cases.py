"""Edge cases and optional-column behavior for Gold aggregations."""

from pyspark.sql import SparkSession

from spark_jobs.silver_to_gold import (
    compute_driver_race_summary,
    compute_driver_telemetry,
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
    empty_car_data = spark.createDataFrame([], "session_key int, driver_number int")

    assert compute_driver_race_summary(empty_laps, empty_drivers, empty_stints, empty_pit).count() == 0
    assert compute_tyre_degradation(empty_laps, empty_drivers, empty_stints).count() == 0
    assert compute_pit_strategy(empty_stints, empty_pit, empty_drivers).count() == 0
    assert compute_weather_impact(empty_weather, empty_laps).count() == 0
    assert compute_driver_telemetry(empty_car_data, empty_drivers).count() == 0


def test_pit_stop_threshold_edge_cases(spark: SparkSession):
    from spark_jobs.bronze_to_silver import transform_fct_pit
    raw = [
        # 1. 16.9 s null-stop row not counted
        {"session_key": 9693, "driver_number": 23, "lap_number": 3, "pit_duration": 16.9, "stop_duration": None},
        # 2. 19.4 s null-stop row counted
        {"session_key": 9979, "driver_number": 63, "lap_number": 53, "pit_duration": 19.4, "stop_duration": None},
        # 3. Exactly 18.0 s null-stop row counted
        {"session_key": 9999, "driver_number": 10, "lap_number": 15, "pit_duration": 18.0, "stop_duration": None},
        # 4. stop_duration 2.1 s with 13 s pit_duration counted
        {"session_key": 9999, "driver_number": 44, "lap_number": 20, "pit_duration": 13.0, "stop_duration": 2.1},
    ]
    df = transform_fct_pit(spark.createDataFrame(raw), threshold_seconds=18.0)
    rows = {r["driver_number"]: r for r in df.collect()}

    # Check 1
    assert rows[23]["is_counted_stop"] is False
    assert rows[23]["stop_detection_method"] == "not_counted"

    # Check 2
    assert rows[63]["is_counted_stop"] is True
    assert rows[63]["stop_detection_method"] == "pit_duration_threshold"

    # Check 3
    assert rows[10]["is_counted_stop"] is True
    assert rows[10]["stop_detection_method"] == "pit_duration_threshold"

    # Check 4
    assert rows[44]["is_counted_stop"] is True
    assert rows[44]["stop_detection_method"] == "stop_duration"


def test_race_results_and_driver_summary_stop_totals_equal():
    """Verify total_pit_stops equals between race_results and driver_summary for every driver across all races."""
    import json
    from pathlib import Path

    gold_path = Path("dashboard/gold_data.json")
    with gold_path.open(encoding="utf-8") as f:
        data = json.load(f)

    rr_map = {(r["session_key"], r["driver_number"]): r.get("total_pit_stops") for r in data.get("race_results", [])}
    ds_map = {(r["session_key"], r["driver_number"]): r.get("total_pit_stops") for r in data.get("driver_summary", [])}

    # Every driver in race_results should match driver_summary
    for key, rr_stops in rr_map.items():
        if key in ds_map:
            assert rr_stops == ds_map[key], f"Mismatch for session {key[0]} driver {key[1]}: race_results={rr_stops} vs driver_summary={ds_map[key]}"


def test_strategy_stints_grouping_and_chips():
    """Verify strategy stints for Australia drivers 4 & 1 and Bahrain Bottas & Hulkenberg."""
    import json
    from pathlib import Path

    gold_path = Path("dashboard/gold_data.json")
    with gold_path.open(encoding="utf-8") as f:
        data = json.load(f)

    stints = data.get("pit_strategy", [])

    def get_chips(session_key, driver_number):
        driver_stints = [s for s in stints if s.get("session_key") == session_key and s.get("driver_number") == driver_number]
        # Segments by strategy_stint_number taking last compound
        segs = {}
        for s in sorted(driver_stints, key=lambda x: x.get("stint_number", 0)):
            sid = s.get("strategy_stint_number") or s.get("stint_number")
            segs[sid] = (s.get("compound") or "")[0].upper()
        return " > ".join(segs[k] for k in sorted(segs.keys()))

    # 9693 Drivers 4 and 1
    assert get_chips(9693, 4) == "I > H > I"
    assert get_chips(9693, 1) == "I > M > I"

    # 9472 Bottas and Hulkenberg (same-compound stops must produce distinct strategy stints)
    assert get_chips(9472, 77) == "S > H > H"
    assert get_chips(9472, 27) == "S > H > H > S"

