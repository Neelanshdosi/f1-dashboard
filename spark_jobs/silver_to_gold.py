"""
silver_to_gold.py
─────────────────
PySpark job that reads cleaned Silver Parquet tables from GCS,
computes race-strategy analytics (driver summaries, tyre degradation,
pit stop strategies, and weather impact), writes Gold Parquet to GCS,
and loads the tables directly into BigQuery dataset `f1_analytics`.
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


def get_spark_session(app_name: str = "f1_silver_to_gold") -> SparkSession:
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
            .config("spark.default.parallelism", "1")
            .config("spark.sql.shuffle.partitions", "1")
        )
    builder = builder.config("spark.sql.adaptive.enabled", "false").config(
        "spark.sql.parquet.compression.codec", "snappy"
    )
    return builder.getOrCreate()


# ─────────────────────────────────────────────────────────────────────────────
# Gold Aggregation Functions (Pure & Testable)
# ─────────────────────────────────────────────────────────────────────────────


def compute_driver_race_summary(
    df_laps: DataFrame,
    df_drivers: DataFrame,
    df_stints: DataFrame,
    df_pit: DataFrame,
) -> DataFrame:
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType

    df_laps = _ensure_optional_columns(
        df_laps,
        {
            "meeting_key": (None, "int"),
            "is_pit_out_lap": (False, "boolean"),
            "lap_time_seconds": (None, "double"),
        },
    )
    df_drivers = _ensure_optional_columns(
        df_drivers,
        {
            "full_name": ("", "string"),
            "name_acronym": ("", "string"),
            "team_name": ("", "string"),
            "team_colour": ("", "string"),
        },
    )
    df_stints = _ensure_optional_columns(df_stints, {"compound": ("UNKNOWN", "string")})
    df_pit = _ensure_optional_columns(df_pit, {"pit_duration": (None, "double")})

    # Lap stats (exclude out laps and extreme anomalies for avg pace)
    valid_laps = df_laps.filter(
        (~F.col("is_pit_out_lap")) & (F.col("lap_time_seconds").isNotNull())
    )
    lap_stats = valid_laps.groupBy("session_key", "driver_number").agg(
        F.round(F.min("lap_time_seconds"), 3).alias("fastest_lap_seconds"),
        F.round(F.avg("lap_time_seconds"), 3).alias("avg_lap_seconds"),
    )

    total_laps_df = df_laps.groupBy("session_key", "driver_number").agg(
        F.max("lap_number").cast(IntegerType()).alias("total_laps"),
        F.first("meeting_key").cast(IntegerType()).alias("meeting_key"),
    )

    # Pit stop stats
    pit_stats = df_pit.groupBy("session_key", "driver_number").agg(
        F.count("lap_number").cast(IntegerType()).alias("total_pit_stops"),
        F.round(F.coalesce(F.sum("pit_duration"), F.lit(0.0)), 2).alias("total_pit_duration"),
    )

    # Stint stats: starting compound (stint 1) and max stint length
    stint1 = (
        df_stints.filter(F.col("stint_number") == 1)
        .select("session_key", "driver_number", F.col("compound").alias("starting_compound"))
    )
    max_stint = (
        df_stints.withColumn("stint_len", F.col("lap_end") - F.col("lap_start") + 1)
        .groupBy("session_key", "driver_number")
        .agg(F.max("stint_len").cast(IntegerType()).alias("max_stint_length"))
    )

    # Combine driver info and metrics
    drivers_sel = df_drivers.select(
        "session_key",
        "driver_number",
        F.col("full_name").alias("driver_name"),
        "name_acronym",
        "team_name",
        "team_colour",
    )

    summary = (
        drivers_sel.join(total_laps_df, on=["session_key", "driver_number"], how="left")
        .join(lap_stats, on=["session_key", "driver_number"], how="left")
        .join(pit_stats, on=["session_key", "driver_number"], how="left")
        .join(stint1, on=["session_key", "driver_number"], how="left")
        .join(max_stint, on=["session_key", "driver_number"], how="left")
        .fillna(
            {
                "total_pit_stops": 0,
                "total_pit_duration": 0.0,
                "starting_compound": "UNKNOWN",
                "max_stint_length": 0,
            }
        )
        .select(
            "session_key",
            "meeting_key",
            "driver_number",
            "driver_name",
            "name_acronym",
            "team_name",
            "team_colour",
            "total_laps",
            "fastest_lap_seconds",
            "avg_lap_seconds",
            "total_pit_stops",
            "total_pit_duration",
            "starting_compound",
            "max_stint_length",
        )
        .dropDuplicates(["session_key", "driver_number"])
    )
    return summary


def compute_tyre_degradation(
    df_laps: DataFrame,
    df_drivers: DataFrame,
    df_stints: DataFrame,
) -> DataFrame:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    df_laps = _ensure_optional_columns(
        df_laps,
        {
            "is_pit_out_lap": (False, "boolean"),
            "lap_time_seconds": (None, "double"),
            "stint_number": (None, "int"),
            "tyre_compound": (None, "string"),
            "tyre_life": (None, "int"),
            "lap_number": (None, "int"),
        },
    )
    df_drivers = _ensure_optional_columns(
        df_drivers,
        {"full_name": ("", "string"), "team_name": ("", "string")},
    )

    # Filter clean laps with valid tyre info
    clean_laps = df_laps.filter(
        (~F.col("is_pit_out_lap"))
        & (F.col("lap_time_seconds").isNotNull())
        & (F.col("tyre_life").isNotNull())
    )

    w = Window.partitionBy("session_key", "driver_number", "stint_number")

    deg = (
        clean_laps.withColumn("stint_avg_lap_time", F.round(F.avg("lap_time_seconds").over(w), 3))
        .withColumn(
            "delta_to_stint_avg",
            F.round(F.col("lap_time_seconds") - F.col("stint_avg_lap_time"), 3),
        )
        .join(
            df_drivers.select(
                "session_key",
                "driver_number",
                F.col("full_name").alias("driver_name"),
                "team_name",
            ),
            on=["session_key", "driver_number"],
            how="left",
        )
        .select(
            "session_key",
            "driver_number",
            "driver_name",
            "team_name",
            F.col("stint_number").cast("int"),
            F.col("tyre_compound").alias("compound"),
            F.col("tyre_life").cast("int").alias("tyre_age"),
            F.col("lap_number").cast("int"),
            F.col("lap_time_seconds").cast("double"),
            "stint_avg_lap_time",
            "delta_to_stint_avg",
        )
        .dropDuplicates(["session_key", "driver_number", "lap_number"])
    )
    return deg


def compute_pit_strategy(
    df_stints: DataFrame,
    df_pit: DataFrame,
    df_drivers: DataFrame,
) -> DataFrame:
    from pyspark.sql import functions as F

    df_pit = _ensure_optional_columns(
        df_pit,
        {"pit_duration": (None, "double"), "stop_duration": (None, "double")},
    )
    df_stints = _ensure_optional_columns(
        df_stints,
        {"compound": ("UNKNOWN", "string")},
    )
    df_drivers = _ensure_optional_columns(
        df_drivers,
        {"full_name": ("", "string"), "team_name": ("", "string")},
    )

    stints_ext = df_stints.withColumn(
        "stint_length", F.col("lap_end") - F.col("lap_start") + 1
    )

    # Join pit info where pit lap equals stint lap_end
    pit_renamed = df_pit.select(
        F.col("session_key").alias("p_session_key"),
        F.col("driver_number").alias("p_driver_number"),
        F.col("lap_number").alias("pit_lap"),
        "pit_duration",
        "stop_duration",
    )

    cond = [
        stints_ext["session_key"] == pit_renamed["p_session_key"],
        stints_ext["driver_number"] == pit_renamed["p_driver_number"],
        stints_ext["lap_end"] == pit_renamed["pit_lap"],
    ]

    joined = (
        stints_ext.join(pit_renamed, on=cond, how="left")
        .join(
            df_drivers.select(
                "session_key",
                "driver_number",
                F.col("full_name").alias("driver_name"),
                "team_name",
            ),
            on=["session_key", "driver_number"],
            how="left",
        )
        .select(
            "session_key",
            "driver_number",
            "driver_name",
            "team_name",
            F.col("stint_number").cast("int"),
            "compound",
            F.col("lap_start").cast("int"),
            F.col("lap_end").cast("int"),
            F.col("stint_length").cast("int"),
            F.col("pit_lap").cast("int"),
            F.round(F.col("pit_duration"), 2).alias("pit_duration"),
            F.round(F.col("stop_duration"), 2).alias("stop_duration"),
        )
        .dropDuplicates(["session_key", "driver_number", "stint_number"])
    )
    return joined


def compute_weather_impact(
    df_weather: DataFrame,
    df_laps: DataFrame,
) -> DataFrame:
    from pyspark.sql import functions as F

    df_weather = _ensure_optional_columns(
        df_weather,
        {
            "date_utc": (None, "timestamp"),
            "air_temperature": (None, "double"),
            "track_temperature": (None, "double"),
            "humidity": (None, "double"),
            "pressure": (None, "double"),
            "wind_speed": (None, "double"),
            "wind_direction": (None, "int"),
            "rainfall": (None, "int"),
        },
    )
    df_laps = _ensure_optional_columns(
        df_laps,
        {
            "is_pit_out_lap": (False, "boolean"),
            "lap_time_seconds": (None, "double"),
        },
    )

    # Field average lap time across entire race
    avg_field_time = (
        df_laps.filter((~F.col("is_pit_out_lap")) & (F.col("lap_time_seconds").isNotNull()))
        .groupBy("session_key")
        .agg(F.round(F.avg("lap_time_seconds"), 3).alias("field_avg_lap_time"))
    )

    impact = (
        df_weather.join(avg_field_time, on="session_key", how="left")
        .select(
            "session_key",
            "date_utc",
            "air_temperature",
            "track_temperature",
            "humidity",
            "pressure",
            "wind_speed",
            "wind_direction",
            "rainfall",
            "field_avg_lap_time",
        )
        .dropDuplicates(["session_key", "date_utc"])
    )
    return impact


def compute_driver_telemetry(
    df_car_data: DataFrame,
    df_drivers: DataFrame,
) -> DataFrame:
    """Compute aggregated driver telemetry metrics from car_data and drivers.

    OpenF1 car_data 'drs' values:
    - 0 or 1: DRS off
    - 8: DRS detected (eligible for activation in DRS zone)
    - 9, 10, 12, 14: DRS open / active deployment
    Assumption: DRS is considered 'open' when drs in (9, 10, 12, 14).
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegerType

    df_car_data = _ensure_optional_columns(
        df_car_data,
        {
            "meeting_key": (None, "int"),
            "speed": (0, "int"),
            "throttle": (0, "int"),
            "brake": (0, "int"),
            "drs": (0, "int"),
        },
    )
    df_drivers = _ensure_optional_columns(
        df_drivers,
        {
            "full_name": ("", "string"),
            "team_name": ("", "string"),
            "team_colour": ("", "string"),
        },
    )

    valid_car_data = df_car_data.filter(
        F.col("session_key").isNotNull()
        & F.col("driver_number").isNotNull()
    )

    # OpenF1 DRS values: 0/1=off, 8=detected, 9/10/12/14=open
    drs_open_condition = F.col("drs").isin([9, 10, 12, 14])

    telemetry_agg = valid_car_data.groupBy("session_key", "driver_number").agg(
        F.first("meeting_key").cast(IntegerType()).alias("meeting_key"),
        F.max("speed").cast(IntegerType()).alias("max_speed"),
        F.round(F.avg("speed"), 2).alias("avg_speed"),
        F.round(F.avg("throttle"), 2).alias("avg_throttle"),
        F.round(
            F.avg(F.when(F.col("throttle") == 100, 100.0).otherwise(0.0)),
            2,
        ).alias("full_throttle_pct"),
        F.round(
            F.avg(F.when(F.col("brake") > 0, 100.0).otherwise(0.0)),
            2,
        ).alias("brake_pct"),
        F.round(
            F.avg(F.when(drs_open_condition, 100.0).otherwise(0.0)),
            2,
        ).alias("drs_open_pct"),
    )

    drivers_sel = df_drivers.select(
        "session_key",
        "driver_number",
        F.col("full_name").alias("driver_name"),
        "team_name",
        "team_colour",
    ).dropDuplicates(["session_key", "driver_number"])

    result = (
        telemetry_agg.join(drivers_sel, on=["session_key", "driver_number"], how="left")
        .select(
            "session_key",
            "meeting_key",
            "driver_number",
            "driver_name",
            "team_name",
            "team_colour",
            "max_speed",
            "avg_speed",
            "avg_throttle",
            "full_throttle_pct",
            "brake_pct",
            "drs_open_pct",
        )
        .dropDuplicates(["session_key", "driver_number"])
    )
    return result


def compute_race_results(
    df_result: DataFrame | None,
    df_grid: DataFrame | None,
    df_drivers: DataFrame,
    df_stints: DataFrame | None = None,
    df_pit: DataFrame | None = None,
) -> DataFrame:
    """Compute race results per driver per session combining results, starting grid, drivers, stints, and pit stops.

    Status evaluation rule (strictly in order):
      1. dsq is True -> "DSQ"
      2. dns is True -> "DNS"
      3. dnf is True -> "DNF"
      4. position is not null -> "Finished" (lapped drivers are Finished; see laps_down)
      5. otherwise (including drivers with no result row) -> "Unclassified"
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
    )

    # 1. Base drivers
    df_drivers = _ensure_optional_columns(
        df_drivers,
        {
            "session_key": (None, "int"),
            "driver_number": (None, "int"),
            "full_name": ("", "string"),
            "name_acronym": ("", "string"),
            "team_name": ("", "string"),
            "team_colour": ("", "string"),
        },
    )
    drivers_base = df_drivers.select(
        F.col("session_key").cast(IntegerType()),
        F.col("driver_number").cast(IntegerType()),
        F.col("full_name").alias("driver_name"),
        "name_acronym",
        "team_name",
        "team_colour",
    ).dropDuplicates(["session_key", "driver_number"])

    # 2. Results DataFrame
    if df_result is not None:
        df_result = _ensure_optional_columns(
            df_result,
            {
                "session_key": (None, "int"),
                "driver_number": (None, "int"),
                "position": (None, "int"),
                "number_of_laps": (None, "int"),
                "points": (None, "double"),
                "dnf": (False, "boolean"),
                "dns": (False, "boolean"),
                "dsq": (False, "boolean"),
                "duration_seconds": (None, "double"),
                "gap_to_leader_seconds": (None, "double"),
                "laps_down": (None, "int"),
            },
        )
        res_sel = df_result.select(
            F.col("session_key").cast(IntegerType()),
            F.col("driver_number").cast(IntegerType()),
            F.col("position").cast(IntegerType()).alias("final_position"),
            F.col("number_of_laps").cast(IntegerType()),
            F.col("points").cast(DoubleType()),
            F.coalesce(F.col("dnf").cast(BooleanType()), F.lit(False)).alias("dnf"),
            F.coalesce(F.col("dns").cast(BooleanType()), F.lit(False)).alias("dns"),
            F.coalesce(F.col("dsq").cast(BooleanType()), F.lit(False)).alias("dsq"),
            F.col("duration_seconds").cast(DoubleType()),
            F.col("gap_to_leader_seconds").cast(DoubleType()),
            F.col("laps_down").cast(IntegerType()),
            F.lit(True).alias("_has_result"),
        ).dropDuplicates(["session_key", "driver_number"])
    else:
        # Dummy empty results schema
        res_sel = drivers_base.select(
            "session_key",
            "driver_number",
            F.lit(None).cast(IntegerType()).alias("final_position"),
            F.lit(None).cast(IntegerType()).alias("number_of_laps"),
            F.lit(None).cast(DoubleType()).alias("points"),
            F.lit(False).alias("dnf"),
            F.lit(False).alias("dns"),
            F.lit(False).alias("dsq"),
            F.lit(None).cast(DoubleType()).alias("duration_seconds"),
            F.lit(None).cast(DoubleType()).alias("gap_to_leader_seconds"),
            F.lit(None).cast(IntegerType()).alias("laps_down"),
            F.lit(False).alias("_has_result"),
        ).limit(0)

    # 3. Starting Grid DataFrame
    if df_grid is not None:
        df_grid = _ensure_optional_columns(
            df_grid,
            {
                "session_key": (None, "int"),
                "driver_number": (None, "int"),
                "position": (None, "int"),
                "grid_source_session_key": (None, "int"),
            },
        )
        grid_sel = df_grid.select(
            F.col("session_key").cast(IntegerType()),
            F.col("driver_number").cast(IntegerType()),
            F.col("position").cast(IntegerType()).alias("grid_position"),
            F.col("grid_source_session_key").cast(IntegerType()),
        ).dropDuplicates(["session_key", "driver_number"])
    else:
        grid_sel = None

    # 4. Pit stops count
    if df_pit is not None and "session_key" in df_pit.columns and "driver_number" in df_pit.columns:
        pit_counts = (
            df_pit.filter(F.col("session_key").isNotNull() & F.col("driver_number").isNotNull())
            .groupBy("session_key", "driver_number")
            .agg(F.count(F.lit(1)).cast(IntegerType()).alias("total_pit_stops"))
        )
    else:
        pit_counts = None

    # Join drivers with results
    joined = drivers_base.join(res_sel, on=["session_key", "driver_number"], how="left")

    # Join with grid if present
    if grid_sel is not None:
        joined = joined.join(grid_sel, on=["session_key", "driver_number"], how="left")
        grid_missing = F.col("grid_position").isNull()
        grid_pos_col = F.col("grid_position")
        grid_source_col = F.col("grid_source_session_key")
    else:
        grid_missing = F.lit(True)
        grid_pos_col = F.lit(None).cast(IntegerType())
        grid_source_col = F.lit(None).cast(IntegerType())

    # Join with pit counts
    if pit_counts is not None:
        joined = joined.join(pit_counts, on=["session_key", "driver_number"], how="left")
        total_pits_col = F.coalesce(F.col("total_pit_stops"), F.lit(0)).cast(IntegerType())
    else:
        total_pits_col = F.lit(0).cast(IntegerType())

    # Calculate status strictly in order:
    # dsq -> "DSQ", dns -> "DNS", dnf -> "DNF", final_position not null -> "Finished", otherwise -> "Unclassified"
    status_expr = (
        F.when(F.col("dsq") == True, F.lit("DSQ"))  # noqa: E712
        .when(F.col("dns") == True, F.lit("DNS"))  # noqa: E712
        .when(F.col("dnf") == True, F.lit("DNF"))  # noqa: E712
        .when(F.col("final_position").isNotNull(), F.lit("Finished"))
        .otherwise(F.lit("Unclassified"))
    )

    # Calculate positions_gained: grid_position - final_position (null if either is null)
    positions_gained_expr = F.when(
        grid_pos_col.isNotNull() & F.col("final_position").isNotNull(),
        grid_pos_col - F.col("final_position"),
    ).otherwise(F.lit(None).cast(IntegerType()))

    result = (
        joined.withColumn("grid_position", grid_pos_col)
        .withColumn("grid_source_session_key", grid_source_col)
        .withColumn("grid_position_missing", grid_missing.cast(BooleanType()))
        .withColumn("positions_gained", positions_gained_expr)
        .withColumn("status", status_expr.cast(StringType()))
        .withColumn("total_pit_stops", total_pits_col)
        .select(
            "session_key",
            "driver_number",
            "driver_name",
            "name_acronym",
            "team_name",
            "team_colour",
            "grid_position",
            "grid_source_session_key",
            "final_position",
            "positions_gained",
            "status",
            "total_pit_stops",
            "number_of_laps",
            "points",
            "duration_seconds",
            "gap_to_leader_seconds",
            "laps_down",
            "grid_position_missing",
        )
        .dropDuplicates(["session_key", "driver_number"])
    )
    return result


def _ensure_optional_columns(df: DataFrame, defaults: dict[str, tuple[object, str]]) -> DataFrame:
    from pyspark.sql import functions as F

    for name, (default, data_type) in defaults.items():
        if name not in df.columns:
            df = df.withColumn(name, F.lit(default).cast(data_type))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline Runner & BigQuery Exporter
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
    import time
    from datetime import datetime

    start_time = time.time()
    started_at = datetime.now(UTC)

    base = _resolve_base_uri(base_uri)
    is_local = base.startswith("file://")
    print(f"Running Silver to Gold pipeline with base URI: {base} (session={session_key}, season={season})")

    project_id = os.getenv("GCP_PROJECT_ID")
    bq_client = None

    if not is_local:
        from google.cloud import bigquery

        if not project_id:
            raise ValueError("GCP_PROJECT_ID must be set in environment when writing to BigQuery")

        print(f"Connecting to BigQuery project: {project_id}...")
        bq_client = bigquery.Client(project=project_id)
        # Ensure dataset and tables exist
        ensure_gold_tables(bq_client, project_id)
        ensure_audit_table(bq_client, project_id)

    spark = get_spark_session()
    counts: dict[str, int] = {}
    audit_rows: list[dict] = []

    # Step 1: Read Silver Parquet tables with Spark
    print(f"\n--- Reading Silver Parquet files for session={session_key} ---")
    tables = [
        "dim_drivers",
        "dim_sessions",
        "fct_laps",
        "fct_stints",
        "fct_pit",
        "fct_weather",
        "fct_car_data",
        "fct_session_result",
        "fct_starting_grid",
    ]
    silver_dfs: dict[str, DataFrame] = {}

    for tbl in tables:
        path_pattern = f"{base}/silver/{tbl}/season={season}/session_key={session_key}"
        try:
            df_silver = spark.read.parquet(path_pattern)
            if df_silver.rdd.isEmpty():
                print(f"  [WARN] No parquet files found for {tbl} under {path_pattern}")
                continue
            silver_dfs[tbl] = df_silver
            print(f"  Loaded Silver {tbl} from {path_pattern}")
        except Exception as e:
            print(f"  [WARN] Could not read Silver {tbl} at {path_pattern}: {e}")
            continue

    print("\n--- Computing Gold Race Strategy Aggregations ---")

    # Helper function to write Gold table and load to BigQuery
    def _process_gold_table(df: DataFrame, table_name: str, rows_in: int) -> int:
        step_start = time.time()
        count = _write_and_load_gold(
            bq_client=bq_client,
            df=df,
            table_name=table_name,
            season=season,
            session_key=session_key,
            base_uri=base,
            project_id=project_id,
            is_local=is_local,
        )
        audit_rows.append(
            build_audit_row(
                job_name="silver_to_gold",
                session_key=session_key,
                season=season,
                table_name=table_name,
                rows_in=rows_in,
                rows_out=count,
                rows_rejected=0,
                started_at=started_at,
                duration_seconds=round(time.time() - step_start, 3),
                status="SUCCESS",
            )
        )
        return count

    # 1. agg_driver_race_summary
    if all(k in silver_dfs for k in ["fct_laps", "dim_drivers", "fct_stints", "fct_pit"]):
        summary_df = compute_driver_race_summary(
            silver_dfs["fct_laps"],
            silver_dfs["dim_drivers"],
            silver_dfs["fct_stints"],
            silver_dfs["fct_pit"],
        )
        counts["agg_driver_race_summary"] = _process_gold_table(
            summary_df, "agg_driver_race_summary", silver_dfs["fct_laps"].count()
        )

    # 2. agg_tyre_degradation
    if all(k in silver_dfs for k in ["fct_laps", "dim_drivers", "fct_stints"]):
        deg_df = compute_tyre_degradation(
            silver_dfs["fct_laps"],
            silver_dfs["dim_drivers"],
            silver_dfs["fct_stints"],
        )
        counts["agg_tyre_degradation"] = _process_gold_table(
            deg_df, "agg_tyre_degradation", silver_dfs["fct_laps"].count()
        )

    # 3. agg_pit_strategy
    if all(k in silver_dfs for k in ["fct_stints", "fct_pit", "dim_drivers"]):
        pit_strat_df = compute_pit_strategy(
            silver_dfs["fct_stints"],
            silver_dfs["fct_pit"],
            silver_dfs["dim_drivers"],
        )
        counts["agg_pit_strategy"] = _process_gold_table(
            pit_strat_df, "agg_pit_strategy", silver_dfs["fct_stints"].count()
        )

    # 4. agg_weather_impact
    if all(k in silver_dfs for k in ["fct_weather", "fct_laps"]):
        weather_df = compute_weather_impact(
            silver_dfs["fct_weather"],
            silver_dfs["fct_laps"],
        )
        counts["agg_weather_impact"] = _process_gold_table(
            weather_df, "agg_weather_impact", silver_dfs["fct_weather"].count()
        )

    # 5. agg_driver_telemetry
    if all(k in silver_dfs for k in ["fct_car_data", "dim_drivers"]):
        telemetry_df = compute_driver_telemetry(
            silver_dfs["fct_car_data"],
            silver_dfs["dim_drivers"],
        )
        counts["agg_driver_telemetry"] = _process_gold_table(
            telemetry_df, "agg_driver_telemetry", silver_dfs["fct_car_data"].count()
        )

    # 6. agg_race_results
    sr_df = silver_dfs.get("fct_session_result")
    sg_df = silver_dfs.get("fct_starting_grid")
    st_df = silver_dfs.get("fct_stints")
    pit_df = silver_dfs.get("fct_pit")

    if sr_df is None or sr_df.rdd.isEmpty():
        import logging
        logging.getLogger(__name__).error(
            f"Silver fct_session_result is missing or empty for session={session_key}. "
            "Writing NO rows to agg_race_results (never synthesizing results from laps)."
        )
        print(f"  [ERROR] Silver fct_session_result is missing or empty for session={session_key}. Writing NO rows to agg_race_results.")
        audit_rows.append(
            build_audit_row(
                job_name="silver_to_gold",
                session_key=session_key,
                season=season,
                table_name="agg_race_results",
                rows_in=0,
                rows_out=0,
                rows_rejected=0,
                started_at=started_at,
                duration_seconds=0.0,
                status="NO_DATA",
            )
        )
    elif "dim_drivers" in silver_dfs:
        results_df = compute_race_results(
            df_result=sr_df,
            df_grid=sg_df,
            df_drivers=silver_dfs["dim_drivers"],
            df_stints=st_df,
            df_pit=pit_df,
        )
        rows_in = sr_df.count()
        counts["agg_race_results"] = _process_gold_table(
            results_df, "agg_race_results", rows_in
        )

    # 6. Load dim_sessions into BigQuery (unpartitioned)
    if "dim_sessions" in silver_dfs and not is_local:
        from google.cloud import bigquery

        dim_sessions_path = f"{base}/silver/dim_sessions/season={season}/session_key={session_key}/*.parquet"
        load_job = bq_client.load_table_from_uri(
            dim_sessions_path,
            f"{project_id}.f1_analytics.dim_sessions",
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            ),
        )
        load_job.result()
        print(f"  Loaded dim_sessions into BigQuery: {project_id}.f1_analytics.dim_sessions")

    # Record run audit
    _record_audit_log(
        base=base,
        job_name="silver_to_gold",
        session_key=session_key,
        season=season,
        audit_rows=audit_rows,
        is_local=is_local,
        project_id=project_id,
    )

    print("\n" + "=" * 52)
    print("  F1 Gold Aggregation - Phase 3 Summary")
    print(f"  Session : {session_key} (Season {season})")
    print("-" * 52)
    print(f"  {'BigQuery Table':<26} {'Rows Loaded':>12}")
    print("-" * 52)
    for tbl, cnt in sorted(counts.items()):
        print(f"  {tbl:<26} {cnt:>12,}")
    print("-" * 52)
    print(f"  {'TOTAL':<26} {sum(counts.values()):>12,}")
    print(f"  Total Duration: {time.time() - start_time:.2f}s")
    print("=" * 52 + "\n")

    return counts


GOLD_TABLE_SCHEMAS = {
    "agg_driver_race_summary": [
        ("session_key", "INT64"),
        ("meeting_key", "INT64"),
        ("driver_number", "INT64"),
        ("driver_name", "STRING"),
        ("name_acronym", "STRING"),
        ("team_name", "STRING"),
        ("team_colour", "STRING"),
        ("total_laps", "INT64"),
        ("fastest_lap_seconds", "FLOAT64"),
        ("avg_lap_seconds", "FLOAT64"),
        ("total_pit_stops", "INT64"),
        ("total_pit_duration", "FLOAT64"),
        ("starting_compound", "STRING"),
        ("max_stint_length", "INT64"),
    ],
    "agg_tyre_degradation": [
        ("session_key", "INT64"),
        ("driver_number", "INT64"),
        ("driver_name", "STRING"),
        ("team_name", "STRING"),
        ("stint_number", "INT64"),
        ("compound", "STRING"),
        ("tyre_age", "INT64"),
        ("lap_number", "INT64"),
        ("lap_time_seconds", "FLOAT64"),
        ("stint_avg_lap_time", "FLOAT64"),
        ("delta_to_stint_avg", "FLOAT64"),
    ],
    "agg_pit_strategy": [
        ("session_key", "INT64"),
        ("driver_number", "INT64"),
        ("driver_name", "STRING"),
        ("team_name", "STRING"),
        ("stint_number", "INT64"),
        ("compound", "STRING"),
        ("lap_start", "INT64"),
        ("lap_end", "INT64"),
        ("stint_length", "INT64"),
        ("pit_lap", "INT64"),
        ("pit_duration", "FLOAT64"),
        ("stop_duration", "FLOAT64"),
    ],
    "agg_weather_impact": [
        ("session_key", "INT64"),
        ("date_utc", "TIMESTAMP"),
        ("air_temperature", "FLOAT64"),
        ("track_temperature", "FLOAT64"),
        ("humidity", "FLOAT64"),
        ("pressure", "FLOAT64"),
        ("wind_speed", "FLOAT64"),
        ("wind_direction", "INT64"),
        ("rainfall", "INT64"),
        ("field_avg_lap_time", "FLOAT64"),
    ],
    "agg_driver_telemetry": [
        ("session_key", "INT64"),
        ("meeting_key", "INT64"),
        ("driver_number", "INT64"),
        ("driver_name", "STRING"),
        ("team_name", "STRING"),
        ("team_colour", "STRING"),
        ("max_speed", "INT64"),
        ("avg_speed", "FLOAT64"),
        ("avg_throttle", "FLOAT64"),
        ("full_throttle_pct", "FLOAT64"),
        ("brake_pct", "FLOAT64"),
        ("drs_open_pct", "FLOAT64"),
    ],
    "agg_race_results": [
        ("session_key", "INT64"),
        ("driver_number", "INT64"),
        ("driver_name", "STRING"),
        ("name_acronym", "STRING"),
        ("team_name", "STRING"),
        ("team_colour", "STRING"),
        ("grid_position", "INT64"),
        ("grid_source_session_key", "INT64"),
        ("final_position", "INT64"),
        ("positions_gained", "INT64"),
        ("status", "STRING"),
        ("total_pit_stops", "INT64"),
        ("number_of_laps", "INT64"),
        ("points", "FLOAT64"),
        ("duration_seconds", "FLOAT64"),
        ("gap_to_leader_seconds", "FLOAT64"),
        ("laps_down", "INT64"),
        ("grid_position_missing", "BOOLEAN"),
    ],
}


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
        ensure_audit_table(bq_client, project_id)
        errors = bq_client.insert_rows_json(table_ref, audit_rows)
        if errors:
            print(f"  [WARN] Failed to insert BigQuery audit rows: {errors}")
        else:
            print(f"  Inserted {len(audit_rows)} audit row(s) into {table_ref}")


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


DIM_SESSIONS_SCHEMA = [
    ("session_key", "INT64"),
    ("meeting_key", "INT64"),
    ("circuit_key", "INT64"),
    ("circuit_short_name", "STRING"),
    ("country_name", "STRING"),
    ("country_code", "STRING"),
    ("location", "STRING"),
    ("session_type", "STRING"),
    ("session_name", "STRING"),
    ("meeting_name", "STRING"),
    ("date_start", "TIMESTAMP"),
    ("date_end", "TIMESTAMP"),
    ("gmt_offset", "STRING"),
    ("year", "INT64"),
    ("season", "INT64"),
]


def ensure_dim_sessions(bq_client, project_id: str, create_dataset: bool = True) -> None:
    """Ensure unpartitioned dim_sessions table exists in BigQuery."""
    from google.cloud import bigquery

    dataset_ref = f"{project_id}.f1_analytics"
    if create_dataset:
        bq_client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)
    table_ref = f"{dataset_ref}.dim_sessions"
    table = bigquery.Table(table_ref)
    table.schema = [
        bigquery.SchemaField(name, field_type) for name, field_type in DIM_SESSIONS_SCHEMA
    ]
    bq_client.create_table(table, exists_ok=True)


def _load_bigquery_config() -> dict:
    import yaml

    config_path = Path(__file__).resolve().parents[1] / "config.yaml"
    with config_path.open(encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)["bigquery"]


def ensure_gold_tables(bq_client, project_id: str) -> None:
    """Ensure all Gold tables exist in BigQuery with proper partitioning and clustering."""
    from google.cloud import bigquery

    dataset_ref = f"{project_id}.f1_analytics"
    bq_client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)
    ensure_dim_sessions(bq_client, project_id, create_dataset=False)

    settings = _load_bigquery_config()
    partition = settings["session_key_partition"]
    gold_tables = settings["gold_tables"]

    for table_name, schema_fields in GOLD_TABLE_SCHEMAS.items():
        table_ref = f"{dataset_ref}.{table_name}"
        table = bigquery.Table(table_ref)
        table.schema = [
            bigquery.SchemaField(name, field_type) for name, field_type in schema_fields
        ]
        table.range_partitioning = bigquery.RangePartitioning(
            field="session_key",
            range_=bigquery.PartitionRange(
                start=partition["start"],
                end=partition["end"],
                interval=partition["interval"],
            ),
        )
        clustering_fields = gold_tables.get(table_name, {}).get("clustering_fields")
        if clustering_fields:
            table.clustering_fields = clustering_fields

        bq_client.create_table(table, exists_ok=True)


def _validate_session_key(df: DataFrame, expected_session_key: int) -> None:
    """Ensure every row matches the expected session_key before loading."""
    from pyspark.sql import functions as F

    mismatched = df.filter(
        (F.col("session_key").isNull()) | (F.col("session_key") != expected_session_key)
    ).count()
    if mismatched > 0:
        raise ValueError(
            f"Found {mismatched} row(s) with invalid session_key, expected {expected_session_key}"
        )


def _write_and_load_gold(
    bq_client,
    df: DataFrame,
    table_name: str,
    season: int,
    session_key: int,
    base_uri: str,
    project_id: str | None = None,
    is_local: bool = False,
) -> int:
    df_cached = df.cache()
    count = df_cached.count()
    if count == 0:
        return 0

    _validate_session_key(df_cached, session_key)

    # Write Gold Parquet using Spark directly
    gold_path = f"{base_uri}/gold/{table_name}/season={season}/session_key={session_key}"
    df_cached.write.mode("overwrite").parquet(gold_path)
    print(f"  Wrote Gold to: {gold_path} ({count:,} rows)")

    if is_local:
        print(f"  [LOCAL MODE] Skipping BigQuery load for {table_name}${session_key} ({count:,} rows)")
        return count

    # Load directly from GCS into BigQuery (partition-scoped destination)
    partition_table_ref = f"{project_id}.f1_analytics.{table_name}${session_key}"
    load_uri = f"{gold_path}/*.parquet"
    load_job = _load_gold_table_from_uri(
        bq_client, load_uri, partition_table_ref
    )
    load_job.result()  # Wait for BQ load job to complete
    print(f"  Loaded into BigQuery: {partition_table_ref} ({count:,} rows)")

    return count


def _build_gold_load_job_config():
    """Build a BigQuery load config for loading into a partition decorator."""
    from google.cloud import bigquery

    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )


def _load_gold_table_from_uri(bq_client, uri: str, partition_table_ref: str):
    """Submit a Gold Parquet load to a partitioned table destination."""
    job_config = _build_gold_load_job_config()
    return bq_client.load_table_from_uri(uri, partition_table_ref, job_config=job_config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="F1 Silver to Gold PySpark & BigQuery Job")
    parser.add_argument("--session-key", type=int, default=9472)
    parser.add_argument("--season", type=int, default=2024)
    parser.add_argument("--base-uri", type=str, default=None, help="Base URI (e.g. gs://bucket or file:///path)")
    args = parser.parse_args()

    run_pipeline(session_key=args.session_key, season=args.season, base_uri=args.base_uri)
