"""Tests for formatting the BigQuery benchmark output."""

from scripts.benchmark_bq import format_markdown_table, reduction_percent


def test_reduction_percent_uses_byte_counts():
    assert reduction_percent(1000, 250) == 75.0
    assert reduction_percent(1000, 1250) == -25.0
    assert reduction_percent(0, 0) is None


def test_markdown_table_formats_counts_and_reduction():
    table = format_markdown_table(
        [
            {
                "query": "Tyre degradation for one race",
                "unpartitioned_bytes": 1_000_000,
                "optimized_bytes": 250_000,
            },
            {
                "query": "Empty table",
                "unpartitioned_bytes": 0,
                "optimized_bytes": 0,
            },
        ]
    )

    assert "| Query | Unpartitioned bytes | Partitioned + clustered bytes | Reduction % |" in table
    assert "| Tyre degradation for one race | 1,000,000 | 250,000 | 75.0% |" in table
    assert "| Empty table | 0 | 0 | N/A |" in table
