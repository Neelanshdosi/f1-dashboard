"""Ensure BigQuery table creation and load jobs use the partition-safe layout."""

from pathlib import Path
from unittest.mock import Mock

import yaml
from google.cloud import bigquery

from spark_jobs.silver_to_gold import (
    _load_gold_table_from_uri,
    _validate_session_key,
    _write_and_load_gold,
    ensure_gold_tables,
)

ROOT = Path(__file__).resolve().parents[1]


def test_ensure_gold_tables_creates_tables_with_partitioning_and_clustering():
    config = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))[
        "bigquery"
    ]
    partition = config["session_key_partition"]
    ddl = (ROOT / "sql" / "ddl.sql").read_text(encoding="utf-8")
    expected_partition_clause = (
        "GENERATE_ARRAY({start}, {end}, {interval})".format(**partition)
    )

    assert (partition["end"] - partition["start"]) // partition["interval"] < 10_000

    bq_client = Mock()
    ensure_gold_tables(bq_client, project_id="test-project")

    # Verify dataset created
    bq_client.create_dataset.assert_called_once()

    # Verify tables created
    created_tables = {
        call.args[0].table_id: call.args[0]
        for call in bq_client.create_table.call_args_list
    }

    for table_name, table_settings in config["gold_tables"].items():
        assert table_name in created_tables
        table = created_tables[table_name]

        # Verify range partitioning on table
        rp = table.range_partitioning
        assert rp.field == "session_key"
        assert rp.range_.start == partition["start"]
        assert rp.range_.end == partition["end"]
        assert rp.range_.interval == partition["interval"]

        # Verify clustering fields on table
        assert table.clustering_fields == table_settings["clustering_fields"]

        # Verify schema exists and matches sql/ddl.sql
        assert len(table.schema) > 0
        table_ddl = ddl.split(f"CREATE TABLE IF NOT EXISTS `f1_analytics.{table_name}`", 1)[1]
        table_ddl = table_ddl.split(";", 1)[0]
        assert expected_partition_clause in table_ddl
        expected_cluster_clause = "CLUSTER BY " + ", ".join(
            table_settings["clustering_fields"]
        )
        assert expected_cluster_clause in table_ddl


def test_gold_load_jobs_use_partition_decorator_and_write_truncate():
    bq_client = Mock()
    partition_table_ref = "test-project.f1_analytics.agg_driver_race_summary$9472"
    _load_gold_table_from_uri(
        bq_client, "gs://test/gold.parquet", partition_table_ref
    )

    bq_client.load_table_from_uri.assert_called_once()
    args, kwargs = bq_client.load_table_from_uri.call_args
    assert args[0] == "gs://test/gold.parquet"
    assert args[1] == partition_table_ref
    assert args[1].endswith("$9472")

    job_config = kwargs["job_config"]
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
    assert job_config.source_format == bigquery.SourceFormat.PARQUET
    # Partition and clustering should NOT be passed on partition load jobs
    assert job_config.range_partitioning is None
    assert job_config.clustering_fields is None


def test_loading_two_different_session_keys_produces_two_different_destinations(spark, monkeypatch):
    project_id = "test-project"
    base_uri = "gs://test-bucket"

    # Test loading session 9472 and session 9473
    for session_key in [9472, 9473]:
        bq_client = Mock()
        load_job = Mock()
        bq_client.load_table_from_uri.return_value = load_job

        df = spark.createDataFrame([{"session_key": session_key, "driver_number": 1}])
        # Monkeypatch DataFrameWriter.parquet so it doesn't try to connect to GCS Hadoop connector
        from pyspark.sql.readwriter import DataFrameWriter
        monkeypatch.setattr(DataFrameWriter, "parquet", lambda self, path: None)

        count = _write_and_load_gold(
            bq_client=bq_client,
            df=df,
            table_name="agg_driver_race_summary",
            season=2024,
            session_key=session_key,
            base_uri=base_uri,
            project_id=project_id,
            is_local=False,
        )

        assert count == 1
        bq_client.load_table_from_uri.assert_called_once()
        dest = bq_client.load_table_from_uri.call_args[0][1]
        assert dest == f"test-project.f1_analytics.agg_driver_race_summary${session_key}"
        assert dest.endswith(f"${session_key}")


def test_agg_race_results_loads_to_partition_scoped_destination(spark, monkeypatch):
    """Assert agg_race_results uses the partition-safe path ending with $<session_key>."""
    from pyspark.sql.readwriter import DataFrameWriter

    session_key = 9662
    project_id = "test-project"
    bq_client = Mock()
    load_job = Mock()
    bq_client.load_table_from_uri.return_value = load_job

    monkeypatch.setattr(DataFrameWriter, "parquet", lambda self, path: None)

    df = spark.createDataFrame([{"session_key": session_key, "driver_number": 1, "final_position": 1}])
    count = _write_and_load_gold(
        bq_client=bq_client,
        df=df,
        table_name="agg_race_results",
        season=2024,
        session_key=session_key,
        base_uri="gs://test-bucket",
        project_id=project_id,
        is_local=False,
    )

    assert count == 1
    bq_client.load_table_from_uri.assert_called_once()
    dest = bq_client.load_table_from_uri.call_args[0][1]
    assert dest == f"test-project.f1_analytics.agg_race_results${session_key}"
    assert dest.endswith(f"${session_key}")


def test_validate_session_key_raises_on_mismatch(spark):
    import pytest

    valid_df = spark.createDataFrame(
        [{"session_key": 9472, "val": 1}, {"session_key": 9472, "val": 2}]
    )
    _validate_session_key(valid_df, 9472)

    invalid_df = spark.createDataFrame(
        [{"session_key": 9472, "val": 1}, {"session_key": 9473, "val": 2}]
    )
    with pytest.raises(ValueError, match="invalid session_key, expected 9472"):
        _validate_session_key(invalid_df, 9472)



