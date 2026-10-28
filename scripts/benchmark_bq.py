"""Benchmark BigQuery bytes processed for unoptimized and Gold tables."""

from __future__ import annotations

import os
import re
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parents[1]
TABLES = (
    "agg_driver_race_summary",
    "agg_tyre_degradation",
    "agg_pit_strategy",
    "agg_weather_impact",
)


def format_bytes(value: int) -> str:
    return f"{value:,}"


def reduction_percent(unpartitioned_bytes: int, optimized_bytes: int) -> float | None:
    if unpartitioned_bytes == 0:
        return None
    return (unpartitioned_bytes - optimized_bytes) / unpartitioned_bytes * 100


def format_markdown_table(rows: list[dict]) -> str:
    lines = [
        "| Query | Unpartitioned bytes | Partitioned + clustered bytes | Reduction % |",
        "|---|---:|---:|---:|",
    ]
    for row in rows:
        reduction = reduction_percent(
            row["unpartitioned_bytes"], row["optimized_bytes"]
        )
        reduction_text = "N/A" if reduction is None else f"{reduction:.1f}%"
        lines.append(
            "| {query} | {unpartitioned} | {optimized} | {reduction} |".format(
                query=row["query"],
                unpartitioned=format_bytes(row["unpartitioned_bytes"]),
                optimized=format_bytes(row["optimized_bytes"]),
                reduction=reduction_text,
            )
        )
    return "\n".join(lines)


def load_query_templates(path: Path) -> list[tuple[str, str, str]]:
    content = path.read_text(encoding="utf-8")
    sections = re.split(r"(?m)^-- Query: (.+)$", content)
    queries = []
    for index in range(1, len(sections), 2):
        name = sections[index].strip()
        sql = sections[index + 1].strip().split(";", 1)[0].strip()
        match = re.search(r"`PROJECT_ID\.f1_analytics\.(\w+)`", sql)
        if not match:
            raise ValueError(f"Could not identify source table for query: {name}")
        queries.append((name, match.group(1), sql))
    return queries


def benchmark(project_id: str) -> list[dict]:
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    dataset = f"{project_id}.f1_analytics"

    # CTAS creates unpartitioned, unclustered comparison copies by default.
    for table_name in TABLES:
        sql = (
            f"CREATE OR REPLACE TABLE `{dataset}.{table_name}_unpartitioned` AS "
            f"SELECT * FROM `{dataset}.{table_name}`"
        )
        client.query(sql).result()

    query_templates = load_query_templates(ROOT / "sql" / "benchmark.sql")
    results = []
    for query_name, table_name, template in query_templates:
        query_results = {}
        for label, table_variant in (
            ("unpartitioned_bytes", f"{table_name}_unpartitioned"),
            ("optimized_bytes", table_name),
        ):
            sql = template.replace("PROJECT_ID", project_id)
            sql = sql.replace(
                f"f1_analytics.{table_name}`",
                f"f1_analytics.{table_variant}`",
                1,
            )
            job = client.query(
                sql,
                job_config=bigquery.QueryJobConfig(
                    dry_run=True,
                    use_query_cache=False,
                ),
            )
            if job.total_bytes_processed is None:
                raise RuntimeError(f"BigQuery returned no byte estimate for {query_name}")
            query_results[label] = int(job.total_bytes_processed)
        results.append({"query": query_name, **query_results})

    report = format_markdown_table(results)
    report_path = ROOT / "docs" / "benchmark.md"
    report_path.write_text(report + "\n", encoding="utf-8")
    print(report)
    print(f"\nSaved benchmark results to {report_path}")
    return results


def main() -> None:
    project_id = os.environ.get("GCP_PROJECT_ID")
    if not project_id:
        raise OSError("Set GCP_PROJECT_ID before running this benchmark.")
    benchmark(project_id)


if __name__ == "__main__":
    main()
