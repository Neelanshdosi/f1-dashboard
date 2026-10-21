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


def run_pipeline(
    session_key: int = 9472,
    season: int = 2024,
    base_uri: str | None = None,
) -> dict[str, int]:
    """Execute the bronze-to-silver transformation pipeline using Spark I/O."""
    base = _resolve_base_uri(base_uri)
    print(f"Running Bronze to Silver pipeline with base URI: {base} (session={session_key}, season={season})")

    spark = get_spark_session()
    counts: dict[str, int] = {}

    endpoints = ["sessions", "drivers", "stints", "laps", "pit", "weather", "car_data"]
    raw_dfs: dict[str, DataFrame] = {}

    from pathlib import Path
    from urllib.parse import urlparse
    from urllib.request import url2pathname

    print(f"\n--- Reading Bronze files for session={session_key} ---")
    for ep in endpoints:
        if ep == "sessions":
            # For sessions, read season files and filter by session_key
            path_pattern = f"{base}/bronze/sessions/season={season}/*/*.json"
        else:
            path_pattern = f"{base}/bronze/{ep}/season={season}/session_key={session_key}/*.json"

        try:
            if base.startswith("file://"):
                # On local Windows, Hadoop globbing fails with NativeIO UnsatisfiedLinkError.
                # Resolve paths in Python first.
                local_root = Path(url2pathname(urlparse(base).path))
                if ep == "sessions":
                    matched_files = list((local_root / "bronze" / "sessions" / f"season={season}").glob("*/*.json"))
                else:
                    matched_files = list((local_root / "bronze" / ep / f"season={season}" / f"session_key={session_key}").glob("*.json"))

                if not matched_files:
                    print(f"  [WARN] No records found for {ep} at {path_pattern}")
                    continue
                file_paths = [str(p) for p in matched_files]
                df_raw = spark.read.json(file_paths)
            else:
                df_raw = spark.read.json(path_pattern)

            # If sessions, filter to the requested session_key
            if ep == "sessions" and "session_key" in df_raw.columns:
                df_raw = df_raw.filter(df_raw.session_key.cast("int") == session_key)

            if df_raw.rdd.isEmpty():
                print(f"  [WARN] Empty dataframe for {ep} at {path_pattern}")
                continue

            raw_dfs[ep] = df_raw
            print(f"  Loaded {ep} from {path_pattern}")
        except Exception as e:
            # Handles path not found or empty glob
            print(f"  [WARN] Could not read {ep} at {path_pattern}: {e}")
            continue

    print("\n--- Applying PySpark Silver Transformations & Writing Directly ---")

    # Helper to write Silver DataFrame using Spark
    def _save_silver(df: DataFrame, table_name: str) -> int:
        df_cached = df.cache()
        cnt = df_cached.count()
        if cnt > 0:
            out_path = f"{base}/silver/{table_name}/season={season}/session_key={session_key}"
            df_cached.write.mode("overwrite").parquet(out_path)
            print(f"  Wrote {table_name}: {out_path} ({cnt:,} rows)")
        return cnt

    # 1. dim_sessions
    if "sessions" in raw_dfs:
        silver_sessions = transform_dim_sessions(raw_dfs["sessions"])
        counts["dim_sessions"] = _save_silver(silver_sessions, "dim_sessions")

    # 2. dim_drivers
    if "drivers" in raw_dfs:
        silver_drivers = transform_dim_drivers(raw_dfs["drivers"])
        counts["dim_drivers"] = _save_silver(silver_drivers, "dim_drivers")

    # 3. fct_stints
    silver_stints: DataFrame | None = None
    if "stints" in raw_dfs:
        silver_stints = transform_fct_stints(raw_dfs["stints"])
        counts["fct_stints"] = _save_silver(silver_stints, "fct_stints")

    # 4. fct_laps (with tyre_life joined)
    if "laps" in raw_dfs:
        silver_laps = transform_fct_laps(raw_dfs["laps"], df_stints=silver_stints)
        counts["fct_laps"] = _save_silver(silver_laps, "fct_laps")

    # 5. fct_pit
    if "pit" in raw_dfs:
        silver_pit = transform_fct_pit(raw_dfs["pit"])
        counts["fct_pit"] = _save_silver(silver_pit, "fct_pit")

    # 6. fct_weather
    if "weather" in raw_dfs:
        silver_weather = transform_fct_weather(raw_dfs["weather"])
        counts["fct_weather"] = _save_silver(silver_weather, "fct_weather")

    # 7. fct_car_data
    if "car_data" in raw_dfs:
        silver_car_data = transform_fct_car_data(raw_dfs["car_data"])
        counts["fct_car_data"] = _save_silver(silver_car_data, "fct_car_data")

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
    print("=" * 52 + "\n")

    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="F1 Bronze to Silver PySpark Job")
    parser.add_argument("--session-key", type=int, default=9472)
    parser.add_argument("--season", type=int, default=2024)
    parser.add_argument("--base-uri", type=str, default=None, help="Base URI (e.g. gs://bucket or file:///path)")
    args = parser.parse_args()

    run_pipeline(session_key=args.session_key, season=args.season, base_uri=args.base_uri)
