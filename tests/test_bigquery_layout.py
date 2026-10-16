"""Ensure BigQuery load jobs use the configured Gold table layout."""

from pathlib import Path
from unittest.mock import Mock

import yaml

from spark_jobs.silver_to_gold import _load_gold_table_from_uri

ROOT = Path(__file__).resolve().parents[1]


def test_load_jobs_use_configured_partitioning_and_clustering():
    config = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))[
        "bigquery"
    ]
    partition = config["session_key_partition"]
    ddl = (ROOT / "sql" / "ddl.sql").read_text(encoding="utf-8")
    expected_partition_clause = (
        "GENERATE_ARRAY({start}, {end}, {interval})".format(**partition)
    )

    assert (partition["end"] - partition["start"]) // partition["interval"] < 10_000

    for table_name, table_settings in config["gold_tables"].items():
        client = Mock()
        _load_gold_table_from_uri(
            client, "gs://test/gold.parquet", f"test-project.f1_analytics.{table_name}", table_name
        )

        kwargs = client.load_table_from_uri.call_args.kwargs
        job_config = kwargs["job_config"]
        range_partitioning = job_config.range_partitioning
        assert range_partitioning.field == "session_key"
        assert range_partitioning.range_.start == partition["start"]
        assert range_partitioning.range_.end == partition["end"]
        assert range_partitioning.range_.interval == partition["interval"]
        assert job_config.clustering_fields == table_settings["clustering_fields"]

        table_ddl = ddl.split(f"CREATE TABLE IF NOT EXISTS `f1_analytics.{table_name}`", 1)[1]
        table_ddl = table_ddl.split(";", 1)[0]
        assert expected_partition_clause in table_ddl
        expected_cluster_clause = "CLUSTER BY " + ", ".join(
            table_settings["clustering_fields"]
        )
        assert expected_cluster_clause in table_ddl
