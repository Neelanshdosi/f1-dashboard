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
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from dotenv import load_dotenv

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def get_spark_session(app_name: str = "f1_silver_to_gold") -> SparkSession:
    """Build or retrieve a PySpark SparkSession configured for local execution."""
    from pyspark.sql import SparkSession

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[1]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.parquet.compression.codec", "snappy")
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


def _ensure_optional_columns(df: DataFrame, defaults: dict[str, tuple[object, str]]) -> DataFrame:
    from pyspark.sql import functions as F

    for name, (default, data_type) in defaults.items():
        if name not in df.columns:
            df = df.withColumn(name, F.lit(default).cast(data_type))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline Runner & BigQuery Exporter
# ─────────────────────────────────────────────────────────────────────────────


def run_pipeline(session_key: int = 9472, season: int = 2024) -> dict[str, int]:
    from google.cloud import bigquery, storage

    load_dotenv()
    bucket_name = os.getenv("GCS_BUCKET")
    project_id = os.getenv("GCP_PROJECT_ID")
    if not bucket_name or not project_id:
        raise ValueError("GCS_BUCKET and GCP_PROJECT_ID must be set in environment")

    print(f"Connecting to GCS bucket: {bucket_name} and BigQuery project: {project_id}...")
    storage_client = storage.Client()
    bq_client = bigquery.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)

    # Ensure dataset exists
    dataset_ref = f"{project_id}.f1_analytics"
    bq_client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)

    spark = get_spark_session()
    counts: dict[str, int] = {}

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        silver_cache = temp_path / "silver"
        gold_out = temp_path / "gold"
        silver_cache.mkdir()
        gold_out.mkdir()

        # Step 1: Download Silver Parquet files
        print(f"\n--- Downloading Silver Parquet files for session={session_key} ---")
        tables = ["dim_drivers", "dim_sessions", "fct_laps", "fct_stints", "fct_pit", "fct_weather"]
        silver_dfs: dict[str, DataFrame] = {}

        for tbl in tables:
            prefix = f"silver/{tbl}/season={season}/session_key={session_key}/"
            blobs = list(bucket.list_blobs(prefix=prefix))
            parquet_blobs = [b for b in blobs if b.name.endswith(".parquet")]
            if not parquet_blobs:
                print(f"  [WARN] No parquet files found for {tbl} under {prefix}")
                continue

            local_file = silver_cache / f"{tbl}.parquet"
            parquet_blobs[0].download_to_filename(str(local_file))
            print(f"  Downloaded {tbl}: {parquet_blobs[0].name} ({local_file.stat().st_size / 1024:.1f} KB)")
            silver_dfs[tbl] = spark.read.parquet(str(local_file))

        print("\n--- Computing Gold Race Strategy Aggregations ---")

        # 1. agg_driver_race_summary
        summary_df = compute_driver_race_summary(
            silver_dfs["fct_laps"],
            silver_dfs["dim_drivers"],
            silver_dfs["fct_stints"],
            silver_dfs["fct_pit"],
        )
        counts["agg_driver_race_summary"] = _write_and_load_gold(
            bucket=bucket,
            bq_client=bq_client,
            df=summary_df,
            table_name="agg_driver_race_summary",
            season=season,
            session_key=session_key,
            temp_path=gold_out,
            project_id=project_id,
        )

        # 2. agg_tyre_degradation
        deg_df = compute_tyre_degradation(
            silver_dfs["fct_laps"],
            silver_dfs["dim_drivers"],
            silver_dfs["fct_stints"],
        )
        counts["agg_tyre_degradation"] = _write_and_load_gold(
            bucket=bucket,
            bq_client=bq_client,
            df=deg_df,
            table_name="agg_tyre_degradation",
            season=season,
            session_key=session_key,
            temp_path=gold_out,
            project_id=project_id,
        )

        # 3. agg_pit_strategy
        pit_strat_df = compute_pit_strategy(
            silver_dfs["fct_stints"],
            silver_dfs["fct_pit"],
            silver_dfs["dim_drivers"],
        )
        counts["agg_pit_strategy"] = _write_and_load_gold(
            bucket=bucket,
            bq_client=bq_client,
            df=pit_strat_df,
            table_name="agg_pit_strategy",
            season=season,
            session_key=session_key,
            temp_path=gold_out,
            project_id=project_id,
        )

        # 4. agg_weather_impact
        weather_df = compute_weather_impact(
            silver_dfs["fct_weather"],
            silver_dfs["fct_laps"],
        )
        counts["agg_weather_impact"] = _write_and_load_gold(
            bucket=bucket,
            bq_client=bq_client,
            df=weather_df,
            table_name="agg_weather_impact",
            season=season,
            session_key=session_key,
            temp_path=gold_out,
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
    print("=" * 52 + "\n")

    return counts


def _write_and_load_gold(
    bucket,
    bq_client,
    df: DataFrame,
    table_name: str,
    season: int,
    session_key: int,
    temp_path: Path,
    project_id: str,
) -> int:
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows = [r.asDict(recursive=True) for r in df.collect()]
    count = len(rows)
    if count == 0:
        return 0

    table = pa.Table.from_pylist(rows)
    local_file = temp_path / f"{table_name}.parquet"
    pq.write_table(table, str(local_file), compression="snappy")

    # Upload Gold Parquet to GCS
    gcs_blob_name = f"gold/{table_name}/season={season}/session_key={session_key}/{table_name}.parquet"
    blob = bucket.blob(gcs_blob_name)
    blob.upload_from_filename(str(local_file))
    print(f"  Uploaded to GCS: gs://{bucket.name}/{gcs_blob_name} ({count:,} rows)")

    # Load directly from GCS into BigQuery
    table_ref = f"{project_id}.f1_analytics.{table_name}"
    gcs_uri = f"gs://{bucket.name}/{gcs_blob_name}"
    load_job = _load_gold_table_from_uri(
        bq_client, gcs_uri, table_ref, table_name
    )
    load_job.result()  # Wait for BQ load job to complete
    print(f"  Loaded into BigQuery: {table_ref} ({count:,} rows)")

    return count


def _build_gold_load_job_config(table_name: str):
    """Build a BigQuery load config from the canonical settings in config.yaml."""
    import yaml
    from google.cloud import bigquery

    config_path = Path(__file__).resolve().parents[1] / "config.yaml"
    with config_path.open(encoding="utf-8") as config_file:
        settings = yaml.safe_load(config_file)["bigquery"]

    partition = settings["session_key_partition"]
    table_settings = settings["gold_tables"][table_name]
    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
        range_partitioning=bigquery.RangePartitioning(
            field="session_key",
            range_=bigquery.PartitionRange(
                start=partition["start"],
                end=partition["end"],
                interval=partition["interval"],
            ),
        ),
        clustering_fields=table_settings["clustering_fields"],
    )


def _load_gold_table_from_uri(bq_client, gcs_uri: str, table_ref: str, table_name: str):
    """Submit a Gold Parquet load with the table's configured layout."""
    job_config = _build_gold_load_job_config(table_name)
    return bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="F1 Silver to Gold PySpark & BigQuery Job")
    parser.add_argument("--session-key", type=int, default=9472)
    parser.add_argument("--season", type=int, default=2024)
    args = parser.parse_args()

    run_pipeline(session_key=args.session_key, season=args.season)
