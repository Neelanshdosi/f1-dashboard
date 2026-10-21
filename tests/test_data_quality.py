from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from spark_jobs.bronze_to_silver import (
    AUDIT_TABLE_SCHEMA,
    _record_audit_log,
    build_audit_row,
    validate_and_split_records,
)
from spark_jobs.silver_to_gold import (
    AUDIT_TABLE_SCHEMA as GOLD_AUDIT_SCHEMA,
)
from spark_jobs.silver_to_gold import (
    build_audit_row as build_gold_audit_row,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def test_audit_row_builder_matches_schema():
    """Verify that build_audit_row produces valid dictionary matching BigQuery audit schema."""
    now = datetime.now(UTC)
    row = build_audit_row(
        job_name="bronze_to_silver",
        session_key=9472,
        season=2024,
        table_name="fct_laps",
        rows_in=100,
        rows_out=98,
        rows_rejected=2,
        started_at=now,
        duration_seconds=1.456,
        status="SUCCESS",
    )

    schema_names = [name for name, _ in AUDIT_TABLE_SCHEMA]
    assert set(row.keys()) == set(schema_names)
    assert row["job_name"] == "bronze_to_silver"
    assert row["session_key"] == 9472
    assert row["season"] == 2024
    assert row["table_name"] == "fct_laps"
    assert row["rows_in"] == 100
    assert row["rows_out"] == 98
    assert row["rows_rejected"] == 2
    assert row["started_at"] == now.isoformat()
    assert row["duration_seconds"] == 1.456
    assert row["status"] == "SUCCESS"

    # Also test gold builder
    gold_row = build_gold_audit_row(
        job_name="silver_to_gold",
        session_key=9472,
        season=2024,
        table_name="agg_driver_race_summary",
        rows_in=50,
        rows_out=20,
    )
    gold_schema_names = [name for name, _ in GOLD_AUDIT_SCHEMA]
    assert set(gold_row.keys()) == set(gold_schema_names)
    assert gold_row["rows_rejected"] == 0
    assert gold_row["status"] == "SUCCESS"


def test_validate_and_split_fct_laps(spark: SparkSession):
    """Test laps validation: null keys, out-of-range lap times quarantined with proper reason."""
    records = [
        {"session_key": 9472, "driver_number": 1, "lap_number": 1, "lap_time_seconds": 91.2},
        {"session_key": 9472, "driver_number": None, "lap_number": 2, "lap_time_seconds": 92.0},
        {"session_key": 9472, "driver_number": 1, "lap_number": None, "lap_time_seconds": 93.0},
        {"session_key": 9472, "driver_number": 1, "lap_number": 4, "lap_time_seconds": 10.0},
        {"session_key": 9472, "driver_number": 1, "lap_number": 5, "lap_time_seconds": 500.0},
    ]
    df = spark.createDataFrame(records)
    quality_cfg = {"lap_time_min_seconds": 50.0, "lap_time_max_seconds": 300.0}

    valid_df, rejected_df = validate_and_split_records(df, "fct_laps", 9472, quality_cfg)

    assert valid_df.count() == 1
    valid_row = valid_df.first()
    assert valid_row["lap_number"] == 1
    assert valid_row["driver_number"] == 1

    assert rejected_df is not None
    assert rejected_df.count() == 4
    assert set(rejected_df.columns) == {
        "source_table",
        "reject_reason",
        "raw_record",
        "session_key",
        "rejected_at",
    }

    reasons = {r["reject_reason"] for r in rejected_df.collect()}
    assert "null_driver_number" in reasons
    assert "null_lap_number" in reasons
    assert "lap_time_out_of_range" in reasons

    # Verify raw_record contains original data
    sample_rej = rejected_df.filter(rejected_df.reject_reason == "lap_time_out_of_range").first()
    raw = json.loads(sample_rej["raw_record"])
    assert raw["lap_time_seconds"] in (10.0, 500.0)


def test_validate_and_split_fct_stints(spark: SparkSession):
    """Test stints validation: null keys and lap_end < lap_start."""
    records = [
        {"session_key": 9472, "driver_number": 1, "stint_number": 1, "lap_start": 1, "lap_end": 20},
        {"session_key": 9472, "driver_number": 1, "stint_number": 2, "lap_start": 25, "lap_end": 10},
        {"session_key": 9472, "driver_number": 1, "stint_number": 3, "lap_start": 0, "lap_end": 10},
    ]
    df = spark.createDataFrame(records)
    quality_cfg = {"stint_lap_min": 1}

    valid_df, rejected_df = validate_and_split_records(df, "fct_stints", 9472, quality_cfg)

    assert valid_df.count() == 1
    assert valid_df.first()["stint_number"] == 1

    assert rejected_df is not None
    assert rejected_df.count() == 2
    for r in rejected_df.collect():
        assert r["reject_reason"] == "invalid_stint_laps"


def test_validate_and_split_fct_pit(spark: SparkSession):
    """Test pit validation: pit_duration bounds check."""
    records = [
        {"session_key": 9472, "driver_number": 44, "lap_number": 15, "pit_duration": 22.5},
        {"session_key": 9472, "driver_number": 44, "lap_number": 30, "pit_duration": 1.2},
        {"session_key": 9472, "driver_number": 44, "lap_number": 45, "pit_duration": 450.0},
    ]
    df = spark.createDataFrame(records)
    quality_cfg = {"pit_duration_min_seconds": 5.0, "pit_duration_max_seconds": 300.0}

    valid_df, rejected_df = validate_and_split_records(df, "fct_pit", 9472, quality_cfg)

    assert valid_df.count() == 1
    assert valid_df.first()["pit_duration"] == 22.5

    assert rejected_df is not None
    assert rejected_df.count() == 2
    for r in rejected_df.collect():
        assert r["reject_reason"] == "pit_duration_out_of_range"


def test_validate_and_split_fct_weather(spark: SparkSession):
    """Test weather validation: temperature bounds check."""
    records = [
        {"session_key": 9472, "date_utc": "2024-05-18 14:00:00", "track_temperature": 35.0, "air_temperature": 25.0},
        {"session_key": 9472, "date_utc": "2024-05-18 14:01:00", "track_temperature": 95.0, "air_temperature": 25.0},
        {"session_key": 9472, "date_utc": "2024-05-18 14:02:00", "track_temperature": 35.0, "air_temperature": -5.0},
    ]
    df = spark.createDataFrame(records)
    quality_cfg = {
        "track_temp_min": 0.0,
        "track_temp_max": 85.0,
        "air_temp_min": 0.0,
        "air_temp_max": 60.0,
    }

    valid_df, rejected_df = validate_and_split_records(df, "fct_weather", 9472, quality_cfg)

    assert valid_df.count() == 1
    assert rejected_df is not None
    assert rejected_df.count() == 2
    reasons = {r["reject_reason"] for r in rejected_df.collect()}
    assert "track_temp_out_of_range" in reasons
    assert "air_temp_out_of_range" in reasons


def test_record_audit_log_local(tmp_path):
    """Test local audit logging writes JSON-lines file correctly."""
    base_uri = f"file://{tmp_path.as_posix()}"
    rows = [
        build_audit_row("bronze_to_silver", 9472, 2024, "fct_laps", 100, 95, 5),
        build_audit_row("silver_to_gold", 9472, 2024, "agg_driver_race_summary", 95, 20, 0),
    ]

    _record_audit_log(
        base=base_uri,
        job_name="bronze_to_silver",
        session_key=9472,
        season=2024,
        audit_rows=rows,
        is_local=True,
    )

    audit_file = tmp_path / "pipeline_run_audit.jsonl"
    assert audit_file.exists()

    with audit_file.open("r", encoding="utf-8") as f:
        lines = [json.loads(line) for line in f if line.strip()]

    assert len(lines) == 2
    assert lines[0]["table_name"] == "fct_laps"
    assert lines[0]["rows_rejected"] == 5
    assert lines[1]["table_name"] == "agg_driver_race_summary"
    assert lines[1]["rows_out"] == 20
