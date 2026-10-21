"""Print row counts in the static Gold dashboard dataset."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_FILE = ROOT / "dashboard" / "gold_data.json"
GOLD_DATASETS = {
    "agg_driver_race_summary": "driver_summary",
    "agg_tyre_degradation": "tyre_degradation",
    "agg_pit_strategy": "pit_strategy",
    "agg_weather_impact": "weather_impact",
    "agg_race_results": "race_results",
}


def collect_stats(data: dict) -> tuple[dict[str, int], int, list[int], list[int]]:
    counts = {table: len(data.get(key, [])) for table, key in GOLD_DATASETS.items()}
    meta = data.get("meta", {})
    races_with_results = meta.get(
        "races_with_results",
        [r["session_key"] for r in data.get("races", []) if r.get("has_results")],
    )
    races_with_grid = meta.get(
        "races_with_grid",
        sorted(
            {
                    r["session_key"]
                    for r in data.get("race_results", [])
                    if r.get("grid_position") is not None
                }
        ),
    )
    return counts, len(data.get("races", [])), races_with_results, races_with_grid


def main() -> None:
    with DATA_FILE.open(encoding="utf-8") as data_file:
        data = json.load(data_file)
    counts, race_count, races_with_results, races_with_grid = collect_stats(data)

    print("Gold dataset rows")
    print("| Dataset | Rows |")
    print("|---|---:|")
    for table, count in counts.items():
        print(f"| `{table}` | {count:,} |")
    print(f"\nRaces: {race_count}")
    print(f"Races with results: {races_with_results}")
    print(f"Races with grid: {races_with_grid}")


if __name__ == "__main__":
    main()
