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
}


def collect_stats(data: dict) -> tuple[dict[str, int], int]:
    counts = {table: len(data.get(key, [])) for table, key in GOLD_DATASETS.items()}
    return counts, len(data.get("races", []))


def main() -> None:
    with DATA_FILE.open(encoding="utf-8") as data_file:
        data = json.load(data_file)
    counts, race_count = collect_stats(data)

    print("Gold dataset rows")
    print("| Dataset | Rows |")
    print("|---|---:|")
    for table, count in counts.items():
        print(f"| `{table}` | {count:,} |")
    print(f"\nRaces: {race_count}")


if __name__ == "__main__":
    main()
