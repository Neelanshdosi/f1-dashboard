"""
bronze_to_silver.py
───────────────────
PySpark job that transforms raw Bronze JSON into typed, deduplicated,
and partitioned Silver Parquet tables on GCS.

Tables produced:
  • dim_sessions
  • dim_drivers
  • fct_stints
  • fct_laps        (joined with stints for tyre_life and tyre_compound)
  • fct_pit
  • fct_weather
  • fct_car_data
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC
from pathlib import Path
from typing import TYPE_CHECKING

from dotenv import load_dotenv

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def get_spark_session(app_name: str = "f1_bronze_to_silver") -> SparkSession:
    """Build a Spark session, retaining spark-submit settings outside local mode."""
    from pyspark.sql import SparkSession

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # On Windows, if HADOOP_HOME is not set but temp/hadoop exists, configure it
    if sys.platform.startswith("win") and not os.environ.get("HADOOP_HOME"):
        temp_hadoop = Path(os.environ.get("TEMP", "C:/Temp")) / "hadoop"
        if (temp_hadoop / "bin" / "winutils.exe").exists():
            os.environ["HADOOP_HOME"] = str(temp_hadoop)
            os.environ["PATH"] = str(temp_hadoop / "bin") + os.pathsep + os.environ.get("PATH", "")

    builder = SparkSession.builder.appName(app_name)
    if os.getenv("SPARK_LOCAL_MODE") == "1":
        builder = (
            builder.master("local[1]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
        )
    builder = builder.config("spark.sql.adaptive.enabled", "false").config(
        "spark.sql.parquet.compression.codec", "snappy"
    )
    return builder.getOrCreate()


# ─────────────────────────────────────────────────────────────────────────────
# Transformation Functions (Pure & Testable)
# ─────────────────────────────────────────────────────────────────────────────


def transform_dim_sessions(df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType

    df = _ensure_optional_columns(
        df,
        {
            "meeting_name": (None, "string"),
        },
    )

    transformed = (
        df.filter(F.col("is_cancelled") == False)  # noqa: E712
        .withColumn("session_key", F.col("session_key").cast(IntegerType()))
        .withColumn("meeting_key", F.col("meeting_key").cast(IntegerType()))
        .withColumn("circuit_key", F.col("circuit_key").cast(IntegerType()))
        .withColumn("circuit_short_name", F.trim(F.col("circuit_short_name")))
        .withColumn("country_name", F.trim(F.col("country_name")))
        .withColumn("country_code", F.trim(F.col("country_code")))
        .withColumn("location", F.trim(F.col("location")))
        .withColumn("session_type", F.trim(F.col("session_type")))
        .withColumn("session_name", F.trim(F.col("session_name")))
        .withColumn("meeting_name", F.trim(F.col("meeting_name")))
        .withColumn("date_start", F.to_timestamp(F.col("date_start")))
        .withColumn("date_end", F.to_timestamp(F.col("date_end")))
        .withColumn("year", F.col("year").cast(IntegerType()))
        .withColumn("season", F.col("year").cast(IntegerType()))
        .select(
            "session_key",
            "meeting_key",
            "circuit_key",
            "circuit_short_name",
            "country_name",
            "country_code",
            "location",
            "session_type",
            "session_name",
            "meeting_name",
            "date_start",
            "date_end",
            "gmt_offset",
            "year",
            "season",
        )
        .dropDuplicates(["session_key"])
    )
    return transformed


def transform_dim_drivers(df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType

    df = _ensure_optional_columns(
        df,
        {
            "meeting_key": (None, "int"),
            "broadcast_name": ("", "string"),
            "full_name": ("", "string"),
            "first_name": ("", "string"),
            "last_name": ("", "string"),
            "name_acronym": ("", "string"),
            "team_name": ("", "string"),
            "team_colour": ("", "string"),
            "country_code": ("", "string"),
            "headshot_url": ("", "string"),
        },
    )

    transformed = (
        df.withColumn("driver_number", F.col("driver_number").cast(IntegerType()))
        .withColumn("session_key", F.col("session_key").cast(IntegerType()))
        .withColumn("meeting_key", F.col("meeting_key").cast(IntegerType()))
        .withColumn("broadcast_name", F.trim(F.col("broadcast_name")))
        .withColumn("full_name", F.trim(F.col("full_name")))
        .withColumn("first_name", F.trim(F.col("first_name")))
        .withColumn("last_name", F.trim(F.col("last_name")))
        .withColumn("name_acronym", F.trim(F.col("name_acronym")))
        .withColumn("team_name", F.trim(F.col("team_name")))
        .withColumn("team_colour", F.trim(F.col("team_colour")))
        .withColumn("country_code", F.trim(F.col("country_code")))
        .withColumn("headshot_url", F.trim(F.col("headshot_url")))
        .select(
            "driver_number",
            "session_key",
            "meeting_key",
            "broadcast_name",
            "full_name",
            "first_name",
            "last_name",
            "name_acronym",
            "team_name",
            "team_colour",
            "country_code",
            "headshot_url",
        )
        .dropDuplicates(["session_key", "driver_number"])
    )
    return transformed


def transform_fct_stints(df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType

    df = _ensure_optional_columns(
        df,
        {
            "meeting_key": (None, "int"),
            "compound": ("UNKNOWN", "string"),
            "tyre_age_at_start": (0, "int"),
        },
    )

    transformed = (
        df.withColumn("session_key", F.col("session_key").cast(IntegerType()))
        .withColumn("meeting_key", F.col("meeting_key").cast(IntegerType()))
        .withColumn("driver_number", F.col("driver_number").cast(IntegerType()))
        .withColumn("stint_number", F.col("stint_number").cast(IntegerType()))
        .withColumn("lap_start", F.col("lap_start").cast(IntegerType()))
        .withColumn("lap_end", F.col("lap_end").cast(IntegerType()))
        .withColumn("compound", F.upper(F.trim(F.col("compound"))))
        .withColumn("tyre_age_at_start", F.col("tyre_age_at_start").cast(IntegerType()))
        .select(
            "session_key",
            "meeting_key",
            "driver_number",
            "stint_number",
            "lap_start",
            "lap_end",
            "compound",
            "tyre_age_at_start",
        )
        .dropDuplicates(["session_key", "driver_number", "stint_number"])
    )
    return transformed


def transform_fct_laps(df_laps: DataFrame, df_stints: DataFrame | None = None) -> DataFrame:
    from pyspark.sql import functions as F
    from pyspark.sql.types import BooleanType, DoubleType, IntegerType

    df_laps = _ensure_optional_columns(
        df_laps,
        {
            "meeting_key": (None, "int"),
            "date_start": (None, "timestamp"),
            "duration_sector_1": (None, "double"),
            "duration_sector_2": (None, "double"),
            "duration_sector_3": (None, "double"),
            "i1_speed": (None, "double"),
            "i2_speed": (None, "double"),
            "st_speed": (None, "double"),
            "is_pit_out_lap": (False, "boolean"),
        },
    )

    cleaned = (
        df_laps.withColumn("session_key", F.col("session_key").cast(IntegerType()))
        .withColumn("meeting_key", F.col("meeting_key").cast(IntegerType()))
        .withColumn("driver_number", F.col("driver_number").cast(IntegerType()))
        .withColumn("lap_number", F.col("lap_number").cast(IntegerType()))
        .withColumn("date_start", F.to_timestamp(F.col("date_start")))
        .withColumn("lap_duration", F.expr("try_cast(lap_duration AS DOUBLE)"))
        .withColumn("lap_time_seconds", F.col("lap_duration").cast(DoubleType()))
        .withColumn("duration_sector_1", F.col("duration_sector_1").cast(DoubleType()))
        .withColumn("duration_sector_2", F.col("duration_sector_2").cast(DoubleType()))
        .withColumn("duration_sector_3", F.col("duration_sector_3").cast(DoubleType()))
        .withColumn("i1_speed", F.col("i1_speed").cast(DoubleType()))
        .withColumn("i2_speed", F.col("i2_speed").cast(DoubleType()))
        .withColumn("st_speed", F.col("st_speed").cast(DoubleType()))
        .withColumn("is_pit_out_lap", F.coalesce(F.col("is_pit_out_lap").cast(BooleanType()), F.lit(False)))
        .dropDuplicates(["session_key", "driver_number", "lap_number"])
    )

    if df_stints is not None:
        cond = [
            cleaned["session_key"] == df_stints["session_key"],
            cleaned["driver_number"] == df_stints["driver_number"],
            cleaned["lap_number"] >= df_stints["lap_start"],
            cleaned["lap_number"] <= df_stints["lap_end"],
        ]
        joined = cleaned.join(df_stints, on=cond, how="left").select(
            cleaned["*"],
            df_stints["stint_number"],
            df_stints["compound"].alias("tyre_compound"),
            (cleaned["lap_number"] - df_stints["lap_start"] + df_stints["tyre_age_at_start"]).alias("tyre_life"),
        )
        return joined

    return cleaned


def transform_fct_pit(df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, IntegerType

    date_col = "date" if "date" in df.columns else "date_start"
    df = _ensure_optional_columns(
        df,
        {
            "meeting_key": (None, "int"),
            "date_start": (None, "timestamp"),
            "pit_duration": (None, "double"),
            "stop_duration": (None, "double"),
            "lane_duration": (None, "double"),
        },
    )
    transformed = (
        df.withColumn("session_key", F.col("session_key").cast(IntegerType()))
        .withColumn("meeting_key", F.col("meeting_key").cast(IntegerType()))
        .withColumn("driver_number", F.col("driver_number").cast(IntegerType()))
        .withColumn("lap_number", F.col("lap_number").cast(IntegerType()))
        .withColumn("date_start", F.to_timestamp(F.col(date_col)))
        .withColumn("pit_duration", F.col("pit_duration").cast(DoubleType()))
        .withColumn("stop_duration", F.col("stop_duration").cast(DoubleType()))
        .withColumn("lane_duration", F.col("lane_duration").cast(DoubleType()))
        .select(
            "session_key",
            "meeting_key",
            "driver_number",
            "lap_number",
            "date_start",
            "pit_duration",
            "stop_duration",
            "lane_duration",
        )
        .dropDuplicates(["session_key", "driver_number", "lap_number"])
    )
    return transformed


def transform_fct_weather(df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, IntegerType

    df = _ensure_optional_columns(
        df,
        {
            "meeting_key": (None, "int"),
            "date": (None, "timestamp"),
            "air_temperature": (None, "double"),
            "track_temperature": (None, "double"),
            "humidity": (None, "double"),
            "pressure": (None, "double"),
            "wind_speed": (None, "double"),
            "wind_direction": (None, "int"),
            "rainfall": (None, "int"),
        },
    )

    transformed = (
        df.withColumn("session_key", F.col("session_key").cast(IntegerType()))
        .withColumn("meeting_key", F.col("meeting_key").cast(IntegerType()))
        .withColumn("date_utc", F.to_timestamp(F.col("date")))
        .withColumn("air_temperature", F.col("air_temperature").cast(DoubleType()))
        .withColumn("track_temperature", F.col("track_temperature").cast(DoubleType()))
        .withColumn("humidity", F.col("humidity").cast(DoubleType()))
        .withColumn("pressure", F.col("pressure").cast(DoubleType()))
        .withColumn("wind_speed", F.col("wind_speed").cast(DoubleType()))
        .withColumn("wind_direction", F.col("wind_direction").cast(IntegerType()))
        .withColumn("rainfall", F.col("rainfall").cast(IntegerType()))
        .select(
            "session_key",
            "meeting_key",
            "date_utc",
            "air_temperature",
            "track_temperature",
            "humidity",
            "pressure",
            "wind_speed",
            "wind_direction",
            "rainfall",
        )
        .dropDuplicates(["session_key", "date_utc"])
    )
    return transformed


def _ensure_optional_columns(df: DataFrame, defaults: dict[str, tuple[object, str]]) -> DataFrame:
    from pyspark.sql import functions as F

    for name, (default, data_type) in defaults.items():
        if name not in df.columns:
            df = df.withColumn(name, F.lit(default).cast(data_type))
    return df


def transform_fct_car_data(df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType

    transformed = (
        df.withColumn("session_key", F.col("session_key").cast(IntegerType()))
        .withColumn("meeting_key", F.col("meeting_key").cast(IntegerType()))
        .withColumn("driver_number", F.col("driver_number").cast(IntegerType()))
        .withColumn("date_utc", F.to_timestamp(F.col("date")))
        .withColumn("rpm", F.col("rpm").cast(IntegerType()))
        .withColumn("speed", F.col("speed").cast(IntegerType()))
        .withColumn("n_gear", F.col("n_gear").cast(IntegerType()))
        .withColumn("throttle", F.col("throttle").cast(IntegerType()))
        .withColumn("brake", F.col("brake").cast(IntegerType()))
        .withColumn("drs", F.col("drs").cast(IntegerType()))
        .select(
            "session_key",
            "meeting_key",
            "driver_number",
            "date_utc",
            "rpm",
            "speed",
            "n_gear",
            "throttle",
            "brake",
            "drs",
        )
        .dropDuplicates(["session_key", "driver_number", "date_utc"])
    )
    return transformed


def transform_fct_session_result(df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
    )

    df = _ensure_optional_columns(
        df,
        {
            "session_key": (None, "int"),
            "meeting_key": (None, "int"),
            "driver_number": (None, "int"),
            "position": (None, "int"),
            "number_of_laps": (None, "int"),
            "points": (None, "double"),
            "dnf": (False, "boolean"),
            "dns": (False, "boolean"),
            "dsq": (False, "boolean"),
            "duration": (None, "string"),
            "gap_to_leader": (None, "string"),
        },
    )

    # Cast duration_seconds: double if scalar numeric string, null if array or null or non-numeric
    is_dur_array = F.col("duration").startswith("[")
    is_dur_numeric = F.col("duration").rlike(r"^[+-]?\d+(\.\d+)?$")
    duration_seconds = (
        F.when(F.col("duration").isNull() | is_dur_array | (~is_dur_numeric), F.lit(None).cast(DoubleType()))
        .otherwise(F.col("duration").cast(DoubleType()))
    )

    # Raw gap string
    gap_to_leader_raw = F.col("gap_to_leader").cast(StringType())

    # gap_to_leader_seconds: numeric gap when parseable, leader=0.0
    is_gap_numeric = F.col("gap_to_leader").rlike(r"^[+-]?\d+(\.\d+)?$")
    gap_to_leader_seconds = (
        F.when(is_gap_numeric, F.col("gap_to_leader").cast(DoubleType()))
        .otherwise(F.lit(None).cast(DoubleType()))
    )

    # laps_down: parsed from "+1 LAP", "+2 LAPS", etc.
    extracted_laps = F.regexp_extract(F.col("gap_to_leader"), r"^\+(\d+)\s+LAP", 1)
    laps_down = (
        F.when(extracted_laps != "", extracted_laps.cast(IntegerType()))
        .otherwise(F.lit(None).cast(IntegerType()))
    )

    transformed = (
        df.withColumn("session_key", F.col("session_key").cast(IntegerType()))
        .withColumn("meeting_key", F.col("meeting_key").cast(IntegerType()))
        .withColumn("driver_number", F.col("driver_number").cast(IntegerType()))
        .withColumn("position", F.col("position").cast(IntegerType()))
        .withColumn("number_of_laps", F.col("number_of_laps").cast(IntegerType()))
        .withColumn("points", F.col("points").cast(DoubleType()))
        .withColumn("dnf", F.coalesce(F.col("dnf").cast(BooleanType()), F.lit(False)))
        .withColumn("dns", F.coalesce(F.col("dns").cast(BooleanType()), F.lit(False)))
        .withColumn("dsq", F.coalesce(F.col("dsq").cast(BooleanType()), F.lit(False)))
        .withColumn("duration_seconds", duration_seconds)
        .withColumn("gap_to_leader_raw", gap_to_leader_raw)
        .withColumn("gap_to_leader_seconds", gap_to_leader_seconds)
        .withColumn("laps_down", laps_down)
        .select(
            "session_key",
            "meeting_key",
            "driver_number",
            "position",
            "number_of_laps",
            "points",
            "dnf",
            "dns",
            "dsq",
            "duration_seconds",
            "gap_to_leader_raw",
            "gap_to_leader_seconds",
            "laps_down",
        )
        .dropDuplicates(["session_key", "driver_number"])
    )
    return transformed


def transform_fct_starting_grid(df: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, IntegerType

    df = _ensure_optional_columns(
        df,
        {
            "session_key": (None, "int"),
            "meeting_key": (None, "int"),
            "driver_number": (None, "int"),
            "position": (None, "int"),
            "lap_duration": (None, "double"),
        },
    )

    transformed = (
        df.withColumn("session_key", F.col("session_key").cast(IntegerType()))
        .withColumn("meeting_key", F.col("meeting_key").cast(IntegerType()))
        .withColumn("driver_number", F.col("driver_number").cast(IntegerType()))
        .withColumn("position", F.col("position").cast(IntegerType()))
        .withColumn("lap_duration", F.col("lap_duration").cast(DoubleType()))
        .select(
            "session_key",
            "meeting_key",
            "driver_number",
            "position",
            "lap_duration",
        )
        .dropDuplicates(["session_key", "driver_number"])
    )
    return transformed


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline Runner
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# Data Quality & Validation
# ─────────────────────────────────────────────────────────────────────────────


def _load_quality_config() -> dict:
    import yaml

    config_path = Path(__file__).resolve().parents[1] / "config.yaml"
    with config_path.open(encoding="utf-8") as config_file:
        cfg = yaml.safe_load(config_file)
        raw_quality = cfg.get("quality", {})
        quality = dict(raw_quality)
        if "lap_time_seconds" in raw_quality and isinstance(raw_quality["lap_time_seconds"], list):
            quality["lap_time_min_seconds"] = raw_quality["lap_time_seconds"][0]
            quality["lap_time_max_seconds"] = raw_quality["lap_time_seconds"][1]
        if "stint_lap_start" in raw_quality:
            quality["stint_lap_min"] = raw_quality["stint_lap_start"]
        if "pit_duration_seconds" in raw_quality and isinstance(raw_quality["pit_duration_seconds"], list):
            quality["pit_duration_min_seconds"] = raw_quality["pit_duration_seconds"][0]
            quality["pit_duration_max_seconds"] = raw_quality["pit_duration_seconds"][1]
        if "track_temperature" in raw_quality and isinstance(raw_quality["track_temperature"], list):
            quality["track_temp_min"] = raw_quality["track_temperature"][0]
            quality["track_temp_max"] = raw_quality["track_temperature"][1]
        if "air_temperature" in raw_quality and isinstance(raw_quality["air_temperature"], list):
            quality["air_temp_min"] = raw_quality["air_temperature"][0]
            quality["air_temp_max"] = raw_quality["air_temperature"][1]
        return quality


def validate_and_split_records(
    df: DataFrame,
    table_name: str,
    session_key: int,
    quality_cfg: dict | None = None,
) -> tuple[DataFrame, DataFrame | None]:
    """Split dataframe into valid rows and rejected records according to quality rules."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    cfg = quality_cfg or _load_quality_config()

    # Rule checks per table
    reject_reason_col = None

    if table_name == "fct_laps":
        min_lap = cfg.get("lap_time_min_seconds", 50.0)
        max_lap = cfg.get("lap_time_max_seconds", 300.0)
        reject_reason_col = (
            F.when(F.col("session_key").isNull(), F.lit("null_session_key"))
            .when(F.col("driver_number").isNull(), F.lit("null_driver_number"))
            .when(F.col("lap_number").isNull(), F.lit("null_lap_number"))
            .when(
                (F.col("lap_time_seconds").isNotNull())
                & ((F.col("lap_time_seconds") <= 0) | (F.col("lap_time_seconds") < min_lap) | (F.col("lap_time_seconds") > max_lap)),
                F.lit("lap_time_out_of_range"),
            )
        )

    elif table_name == "fct_stints":
        min_stint_lap = cfg.get("stint_lap_min", 1)
        reject_reason_col = (
            F.when(F.col("session_key").isNull(), F.lit("null_session_key"))
            .when(F.col("driver_number").isNull(), F.lit("null_driver_number"))
            .when(F.col("stint_number").isNull(), F.lit("null_stint_number"))
            .when(
                (F.col("lap_start").isNull()) | (F.col("lap_end").isNull()) | (F.col("lap_end") < F.col("lap_start")) | (F.col("lap_start") < min_stint_lap),
                F.lit("invalid_stint_laps"),
            )
        )

    elif table_name == "fct_pit":
        min_pit = cfg.get("pit_duration_min_seconds", 5.0)
        max_pit = cfg.get("pit_duration_max_seconds", 300.0)
        reject_reason_col = (
            F.when(F.col("session_key").isNull(), F.lit("null_session_key"))
            .when(F.col("driver_number").isNull(), F.lit("null_driver_number"))
            .when(F.col("lap_number").isNull(), F.lit("null_lap_number"))
            .when(
                (F.col("pit_duration").isNotNull())
                & ((F.col("pit_duration") <= 0) | (F.col("pit_duration") < min_pit) | (F.col("pit_duration") > max_pit)),
                F.lit("pit_duration_out_of_range"),
            )
        )

    elif table_name == "fct_weather":
        min_track = cfg.get("track_temp_min", 0.0)
        max_track = cfg.get("track_temp_max", 85.0)
        min_air = cfg.get("air_temp_min", 0.0)
        max_air = cfg.get("air_temp_max", 60.0)
        reject_reason_col = (
            F.when(F.col("session_key").isNull(), F.lit("null_session_key"))
            .when(F.col("date_utc").isNull(), F.lit("invalid_timestamp"))
            .when(
                (F.col("track_temperature").isNotNull())
                & ((F.col("track_temperature") < min_track) | (F.col("track_temperature") > max_track)),
                F.lit("track_temp_out_of_range"),
            )
            .when(
                (F.col("air_temperature").isNotNull())
                & ((F.col("air_temperature") < min_air) | (F.col("air_temperature") > max_air)),
                F.lit("air_temp_out_of_range"),
            )
        )

    elif table_name == "dim_drivers":
        reject_reason_col = (
            F.when(F.col("session_key").isNull(), F.lit("null_session_key"))
            .when(F.col("driver_number").isNull(), F.lit("null_driver_number"))
        )

    elif table_name == "dim_sessions":
        reject_reason_col = (
            F.when(F.col("session_key").isNull(), F.lit("null_session_key"))
        )

    elif table_name == "fct_car_data":
        reject_reason_col = (
            F.when(F.col("session_key").isNull(), F.lit("null_session_key"))
            .when(F.col("driver_number").isNull(), F.lit("null_driver_number"))
            .when(F.col("date_utc").isNull(), F.lit("invalid_timestamp"))
        )

    elif table_name in ("fct_session_result", "fct_starting_grid"):
        reject_reason_col = (
            F.when(F.col("session_key").isNull(), F.lit("null_session_key"))
            .when(F.col("driver_number").isNull(), F.lit("null_driver_number"))
        )

    if reject_reason_col is None:
        return df, None

    with_reason = df.withColumn("_reject_reason", reject_reason_col)
    valid_df = with_reason.filter(F.col("_reject_reason").isNull()).drop("_reject_reason")
    rejected_subset = with_reason.filter(F.col("_reject_reason").isNotNull())

    # Build standardized rejected_records format
    # columns: source_table, reject_reason, raw_record (JSON string), session_key, rejected_at
    cols_to_json = list(df.columns)
    rejected_df = rejected_subset.select(
        F.lit(table_name).alias("source_table"),
        F.col("_reject_reason").alias("reject_reason"),
        F.to_json(F.struct(*[F.col(c) for c in cols_to_json])).cast(StringType()).alias("raw_record"),
        F.coalesce(F.col("session_key"), F.lit(session_key)).cast("int").alias("session_key"),
        F.current_timestamp().alias("rejected_at"),
    )

    return valid_df, rejected_df


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline Runner
# ─────────────────────────────────────────────────────────────────────────────


def _resolve_base_uri(base_uri: str | None = None) -> str:
    """Resolve the base URI from CLI arg, environment, or default."""
    if base_uri:
        return base_uri.rstrip("/")
    load_dotenv()
    bucket_name = os.getenv("GCS_BUCKET")
    if bucket_name:
        return f"gs://{bucket_name.strip('/')}"
    raise ValueError("base_uri was not provided and GCS_BUCKET environment variable is not set")


def build_audit_row(
    job_name: str,
    session_key: int,
    season: int,
    table_name: str,
    rows_in: int,
    rows_out: int,
    rows_rejected: int = 0,
    started_at=None,
    duration_seconds: float = 0.0,
    status: str = "SUCCESS",
) -> dict:
    """Construct a validated dictionary representing an entry in pipeline_run_audit."""
    from datetime import datetime

    if started_at is None:
        started_at = datetime.now(UTC)

    started_at_str = started_at.isoformat() if isinstance(started_at, datetime) else str(started_at)

    return {
        "job_name": str(job_name),
        "session_key": int(session_key),
        "season": int(season),
        "table_name": str(table_name),
        "rows_in": int(rows_in),
        "rows_out": int(rows_out),
        "rows_rejected": int(rows_rejected),
        "started_at": started_at_str,
        "duration_seconds": float(duration_seconds),
        "status": str(status),
    }


def _record_audit_log(
    base: str,
    job_name: str,
    session_key: int,
    season: int,
    audit_rows: list[dict],
    is_local: bool,
    project_id: str | None = None,
) -> None:
    """Write run audit entries to BigQuery or local JSON-lines file."""
    import json
    from datetime import datetime

    if not audit_rows:
        return

    if is_local:
        from urllib.parse import urlparse
        from urllib.request import url2pathname

        local_root = Path(url2pathname(urlparse(base).path))
        audit_file = local_root / "pipeline_run_audit.jsonl"
        audit_file.parent.mkdir(parents=True, exist_ok=True)
        with audit_file.open("a", encoding="utf-8") as f:
            for r in audit_rows:
                # Convert timestamps to ISO format string
                row_copy = dict(r)
                if isinstance(row_copy.get("started_at"), datetime):
                    row_copy["started_at"] = row_copy["started_at"].isoformat()
                f.write(json.dumps(row_copy) + "\n")
        print(f"  [LOCAL MODE] Appended {len(audit_rows)} audit row(s) to {audit_file}")
    else:
        from google.cloud import bigquery

        if not project_id:
            project_id = os.getenv("GCP_PROJECT_ID")
        bq_client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.f1_analytics.pipeline_run_audit"
        # Ensure audit table exists
        ensure_audit_table(bq_client, project_id)
        errors = bq_client.insert_rows_json(table_ref, audit_rows)
        if errors:
            print(f"  [WARN] Failed to insert BigQuery audit rows: {errors}")
        else:
            print(f"  Inserted {len(audit_rows)} audit row(s) into {table_ref}")


AUDIT_TABLE_SCHEMA = [
    ("job_name", "STRING"),
    ("session_key", "INT64"),
    ("season", "INT64"),
    ("table_name", "STRING"),
    ("rows_in", "INT64"),
    ("rows_out", "INT64"),
    ("rows_rejected", "INT64"),
    ("started_at", "TIMESTAMP"),
    ("duration_seconds", "FLOAT64"),
    ("status", "STRING"),
]


def ensure_audit_table(bq_client, project_id: str) -> None:
    """Ensure pipeline_run_audit table exists in BigQuery."""
    from google.cloud import bigquery

    dataset_ref = f"{project_id}.f1_analytics"
    bq_client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)
    table_ref = f"{dataset_ref}.pipeline_run_audit"
    table = bigquery.Table(table_ref)
    table.schema = [
        bigquery.SchemaField(name, field_type) for name, field_type in AUDIT_TABLE_SCHEMA
    ]
    bq_client.create_table(table, exists_ok=True)


def run_pipeline(
    session_key: int = 9472,
    season: int = 2024,
    base_uri: str | None = None,
) -> dict[str, int]:
    """Execute the bronze-to-silver transformation pipeline using Spark I/O with quality quarantine & audit."""
    import time
    from datetime import datetime
    from urllib.parse import urlparse
    from urllib.request import url2pathname

    start_time = time.time()
    started_at = datetime.now(UTC)

    base = _resolve_base_uri(base_uri)
    is_local = base.startswith("file://")
    project_id = os.getenv("GCP_PROJECT_ID")
    print(f"Running Bronze to Silver pipeline with base URI: {base} (session={session_key}, season={season})")

    spark = get_spark_session()
    counts: dict[str, int] = {}
    audit_rows: list[dict] = []
    quality_cfg = _load_quality_config()

    endpoints = [
        "sessions",
        "drivers",
        "stints",
        "laps",
        "pit",
        "weather",
        "car_data",
        "session_result",
        "starting_grid",
    ]
    raw_dfs: dict[str, DataFrame] = {}

    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    explicit_schemas = {
        "session_result": StructType(
            [
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
            ]
        ),
        "starting_grid": StructType(
            [
                StructField("session_key", IntegerType(), True),
                StructField("meeting_key", IntegerType(), True),
                StructField("driver_number", IntegerType(), True),
                StructField("position", IntegerType(), True),
                StructField("lap_duration", DoubleType(), True),
            ]
        ),
    }

    print(f"\n--- Reading Bronze files for session={session_key} ---")
    for ep in endpoints:
        if ep == "sessions":
            # For sessions, read season files and filter by session_key
            path_pattern = f"{base}/bronze/sessions/season={season}/*/*.json"
        else:
            path_pattern = f"{base}/bronze/{ep}/season={season}/session_key={session_key}/*.json"

        try:
            reader = spark.read
            if ep in explicit_schemas:
                reader = reader.schema(explicit_schemas[ep]).option("multiline", "true")

            if is_local:
                local_root = Path(url2pathname(urlparse(base).path))
                if ep == "sessions":
                    target_dir = local_root / "bronze" / "sessions" / f"season={season}"
                    matched_files = list(target_dir.glob("*/*.json")) if target_dir.exists() else []
                else:
                    target_dir = local_root / "bronze" / ep / f"season={season}" / f"session_key={session_key}"
                    matched_files = list(target_dir.glob("*.json")) if target_dir.exists() else []

                if not matched_files:
                    if ep == "session_result":
                        print(f"  [ERROR] Required endpoint {ep} not found at {path_pattern}")
                    else:
                        print(f"  [WARN] No records found for {ep} at {path_pattern}")
                    if ep in ("session_result", "starting_grid"):
                        audit_rows.append(
                            build_audit_row(
                                job_name="bronze_to_silver",
                                session_key=session_key,
                                season=season,
                                table_name=f"fct_{ep}",
                                rows_in=0,
                                rows_out=0,
                                rows_rejected=0,
                                started_at=started_at,
                                duration_seconds=0.0,
                                status="NO_DATA",
                            )
                        )
                    continue
                file_paths = [str(p) for p in matched_files]
                df_raw = reader.json(file_paths)
            else:
                # In GCS / remote mode, catch AnalysisException if path does not exist
                from pyspark.errors.exceptions.captured import AnalysisException

                try:
                    df_raw = reader.json(path_pattern)
                except AnalysisException as ae:
                    if ep == "session_result":
                        print(f"  [ERROR] Required endpoint {ep} path not found at {path_pattern}: {ae}")
                    else:
                        print(f"  [WARN] Path not found for {ep} at {path_pattern}: {ae}")
                    if ep in ("session_result", "starting_grid"):
                        audit_rows.append(
                            build_audit_row(
                                job_name="bronze_to_silver",
                                session_key=session_key,
                                season=season,
                                table_name=f"fct_{ep}",
                                rows_in=0,
                                rows_out=0,
                                rows_rejected=0,
                                started_at=started_at,
                                duration_seconds=0.0,
                                status="NO_DATA",
                            )
                        )
                    continue

            # If sessions, filter to the requested session_key
            if ep == "sessions" and "session_key" in df_raw.columns:
                df_raw = df_raw.filter(df_raw.session_key.cast("int") == session_key)

            if df_raw.rdd.isEmpty():
                if ep == "session_result":
                    print(f"  [ERROR] Required endpoint {ep} has empty dataframe at {path_pattern}")
                else:
                    print(f"  [WARN] Empty dataframe for {ep} at {path_pattern}")
                if ep in ("session_result", "starting_grid"):
                    audit_rows.append(
                        build_audit_row(
                            job_name="bronze_to_silver",
                            session_key=session_key,
                            season=season,
                            table_name=f"fct_{ep}",
                            rows_in=0,
                            rows_out=0,
                            rows_rejected=0,
                            started_at=started_at,
                            duration_seconds=0.0,
                            status="NO_DATA",
                        )
                    )
                continue

            raw_dfs[ep] = df_raw
            print(f"  Loaded {ep} from {path_pattern}")
        except Exception as e:
            if ep == "session_result":
                print(f"  [ERROR] Could not read required endpoint {ep} at {path_pattern}: {e}")
            else:
                print(f"  [WARN] Could not read {ep} at {path_pattern}: {e}")
            if ep in ("session_result", "starting_grid"):
                audit_rows.append(
                    build_audit_row(
                        job_name="bronze_to_silver",
                        session_key=session_key,
                        season=season,
                        table_name=f"fct_{ep}",
                        rows_in=0,
                        rows_out=0,
                        rows_rejected=0,
                        started_at=started_at,
                        duration_seconds=0.0,
                        status="NO_DATA",
                    )
                )
            continue

    print("\n--- Applying PySpark Silver Transformations & Quality Quarantine ---")

    def _process_and_save_silver(df_transformed: DataFrame, table_name: str, rows_in: int) -> int:
        step_start = time.time()
        valid_df, rejected_df = validate_and_split_records(
            df_transformed, table_name, session_key, quality_cfg
        )
        valid_cached = valid_df.cache()
        rows_out = valid_cached.count()

        rows_rej = 0
        if rejected_df is not None:
            rej_cached = rejected_df.cache()
            rows_rej = rej_cached.count()
            if rows_rej > 0:
                rej_path = f"{base}/silver/rejected_records/season={season}/session_key={session_key}"
                rej_cached.write.mode("append").parquet(rej_path)
                print(f"  [QUARANTINE] Wrote {rows_rej:,} rejected row(s) to {rej_path}")

        if rows_out > 0:
            out_path = f"{base}/silver/{table_name}/season={season}/session_key={session_key}"
            valid_cached.write.mode("overwrite").parquet(out_path)
            print(f"  Wrote {table_name}: {out_path} ({rows_out:,} rows)")

        audit_rows.append(
            build_audit_row(
                job_name="bronze_to_silver",
                session_key=session_key,
                season=season,
                table_name=table_name,
                rows_in=rows_in,
                rows_out=rows_out,
                rows_rejected=rows_rej,
                started_at=started_at,
                duration_seconds=round(time.time() - step_start, 3),
                status="SUCCESS",
            )
        )
        return rows_out

    # 1. dim_sessions
    if "sessions" in raw_dfs:
        s_raw = raw_dfs["sessions"]
        silver_sessions = transform_dim_sessions(s_raw)
        counts["dim_sessions"] = _process_and_save_silver(silver_sessions, "dim_sessions", s_raw.count())

    # 2. dim_drivers
    if "drivers" in raw_dfs:
        d_raw = raw_dfs["drivers"]
        silver_drivers = transform_dim_drivers(d_raw)
        counts["dim_drivers"] = _process_and_save_silver(silver_drivers, "dim_drivers", d_raw.count())

    # 3. fct_stints
    silver_stints: DataFrame | None = None
    if "stints" in raw_dfs:
        st_raw = raw_dfs["stints"]
        silver_stints_raw = transform_fct_stints(st_raw)
        counts["fct_stints"] = _process_and_save_silver(silver_stints_raw, "fct_stints", st_raw.count())
        # Cache clean stints for laps join
        silver_stints = spark.read.parquet(f"{base}/silver/fct_stints/season={season}/session_key={session_key}")

    # 4. fct_laps (with tyre_life joined)
    if "laps" in raw_dfs:
        l_raw = raw_dfs["laps"]
        silver_laps = transform_fct_laps(l_raw, df_stints=silver_stints)
        counts["fct_laps"] = _process_and_save_silver(silver_laps, "fct_laps", l_raw.count())

    # 5. fct_pit
    if "pit" in raw_dfs:
        p_raw = raw_dfs["pit"]
        silver_pit = transform_fct_pit(p_raw)
        counts["fct_pit"] = _process_and_save_silver(silver_pit, "fct_pit", p_raw.count())

    # 6. fct_weather
    if "weather" in raw_dfs:
        w_raw = raw_dfs["weather"]
        silver_weather = transform_fct_weather(w_raw)
        counts["fct_weather"] = _process_and_save_silver(silver_weather, "fct_weather", w_raw.count())

    # 7. fct_car_data
    if "car_data" in raw_dfs:
        c_raw = raw_dfs["car_data"]
        silver_car_data = transform_fct_car_data(c_raw)
        counts["fct_car_data"] = _process_and_save_silver(silver_car_data, "fct_car_data", c_raw.count())

    # 8. fct_session_result
    if "session_result" in raw_dfs:
        sr_raw = raw_dfs["session_result"]
        silver_session_result = transform_fct_session_result(sr_raw)
        counts["fct_session_result"] = _process_and_save_silver(
            silver_session_result, "fct_session_result", sr_raw.count()
        )

    # 9. fct_starting_grid
    if "starting_grid" in raw_dfs:
        sg_raw = raw_dfs["starting_grid"]
        silver_starting_grid = transform_fct_starting_grid(sg_raw)
        counts["fct_starting_grid"] = _process_and_save_silver(
            silver_starting_grid, "fct_starting_grid", sg_raw.count()
        )

    # Record run audit
    _record_audit_log(
        base=base,
        job_name="bronze_to_silver",
        session_key=session_key,
        season=season,
        audit_rows=audit_rows,
        is_local=is_local,
        project_id=project_id,
    )

    print("\n" + "=" * 52)
    print("  F1 Silver Transformation - Phase 2 Summary")
    print(f"  Session : {session_key} (Season {season})")
    print("-" * 52)
    print(f"  {'Table':<20} {'Cleaned Rows':>12}")
    print("-" * 52)
    for tbl, cnt in sorted(counts.items()):
        print(f"  {tbl:<20} {cnt:>12,}")
    print("-" * 52)
    print(f"  {'TOTAL':<20} {sum(counts.values()):>12,}")
    print(f"  Total Duration: {time.time() - start_time:.2f}s")
    print("=" * 52 + "\n")

    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="F1 Bronze to Silver PySpark Job")
    parser.add_argument("--session-key", type=int, default=9472)
    parser.add_argument("--season", type=int, default=2024)
    parser.add_argument("--base-uri", type=str, default=None, help="Base URI (e.g. gs://bucket or file:///path)")
    args = parser.parse_args()

    run_pipeline(session_key=args.session_key, season=args.season, base_uri=args.base_uri)
