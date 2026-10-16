"""Fetch selected OpenF1 race sessions and rebuild the static dashboard data."""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

DATA_FILES = (
    ROOT / "dashboard" / "gold_data.json",
    ROOT / "docs" / "gold_data.json",
)
API = "https://api.openf1.org/v1"
RACE_NAMES = {
    9693: "2025 Australian Grand Prix",
    10014: "2025 Bahrain Grand Prix",
    9979: "2025 Monaco Grand Prix",
    9947: "2025 British Grand Prix",
    9839: "2025 Abu Dhabi Grand Prix",
}
RACE_DATES = {
    9693: "2025-03-16",
    10014: "2025-04-13",
    9979: "2025-05-25",
    9947: "2025-07-06",
    9839: "2025-12-07",
}
ENDPOINTS = ("drivers", "laps", "stints", "pit", "weather")


def fetch(session: requests.Session, endpoint: str, session_key: int) -> list[dict]:
    # OpenF1 allows 30 requests/minute. Keep this refresh at or below that rate.
    time.sleep(2.1)
    response = session.get(
        f"{API}/{endpoint}", params={"session_key": session_key}, timeout=90
    )
    response.raise_for_status()
    return response.json()


def as_records(df) -> list[dict]:
    return [row.asDict(recursive=True) for row in df.collect()]


def spark_frame(spark, rows: list[dict]):
    # Spark cannot infer the type of fields that are null for every row.
    non_null_fields = {
        key
        for row in rows
        for key, value in row.items()
        if value is not None
    }
    return spark.createDataFrame(
        [{key: value for key, value in row.items() if key in non_null_fields} for row in rows]
    )


def add_defaults(rows: list[dict], fields: tuple[str, ...], default="") -> list[dict]:
    normalized = []
    for row in rows:
        item = dict(row)
        for field in fields:
            if item.get(field) is None:
                item[field] = default
        normalized.append(item)
    return normalized


def write_dashboard_data(dashboard: dict) -> None:
    content = json.dumps(dashboard, indent=2, default=str) + "\n"
    for path in DATA_FILES:
        path.write_text(content, encoding="utf-8")


def main() -> None:
    from spark_jobs.bronze_to_silver import (
        transform_dim_drivers,
        transform_fct_laps,
        transform_fct_pit,
        transform_fct_stints,
        transform_fct_weather,
    )
    from spark_jobs.silver_to_gold import (
        compute_driver_race_summary,
        compute_pit_strategy,
        compute_tyre_degradation,
        compute_weather_impact,
        get_spark_session,
    )

    os.environ["SPARK_LOCAL_MODE"] = "1"
    spark = get_spark_session("f1_dashboard_refresh")
    spark.sparkContext.setLogLevel("ERROR")
    http = requests.Session()
    with DATA_FILES[0].open(encoding="utf-8") as handle:
        dashboard = json.load(handle)

    # Retain the existing Bahrain 2024 session and replace/add these five real 2025 races.
    existing_sessions = {
        race["session_key"]: race for race in dashboard.get("races", [])
    }
    existing_sessions.setdefault(
        9472,
        {
            "session_key": 9472,
            "year": 2024,
            "name": "2024 Bahrain Grand Prix",
            "circuit": "Sakhir",
            "date": "2024-03-02",
        },
    )

    for session_key, race_name in RACE_NAMES.items():
        print(f"Fetching {race_name} ({session_key})", flush=True)
        raw = {endpoint: fetch(http, endpoint, session_key) for endpoint in ENDPOINTS}
        if not raw["drivers"] or not raw["laps"] or not raw["stints"]:
            raise RuntimeError(f"OpenF1 returned incomplete race data for {race_name}")

        driver_fields = (
            "driver_number", "session_key", "meeting_key", "broadcast_name",
            "full_name", "first_name", "last_name", "name_acronym",
            "team_name", "team_colour", "country_code", "headshot_url",
        )
        drivers = transform_dim_drivers(
            spark_frame(spark, add_defaults(raw["drivers"], driver_fields))
        )
        stints = transform_fct_stints(spark_frame(spark, raw["stints"]))
        laps = transform_fct_laps(spark_frame(spark, raw["laps"]), stints)
        pit = transform_fct_pit(spark_frame(spark, raw["pit"])) if raw["pit"] else spark.createDataFrame([], "session_key int, driver_number int, lap_number int, pit_duration double, stop_duration double")
        weather = transform_fct_weather(spark_frame(spark, raw["weather"])) if raw["weather"] else spark.createDataFrame([], "session_key int, date timestamp, air_temperature double, track_temperature double, humidity double, pressure double, wind_speed double, wind_direction int, rainfall int")

        session_data = {
            "driver_summary": as_records(
                compute_driver_race_summary(laps, drivers, stints, pit)
            ),
            "tyre_degradation": as_records(
                compute_tyre_degradation(laps, drivers, stints)
            ),
            "pit_strategy": as_records(compute_pit_strategy(stints, pit, drivers)),
            "weather_impact": as_records(compute_weather_impact(weather, laps)),
        }
        for key, rows in session_data.items():
            dashboard[key] = [
                row
                for row in dashboard.get(key, [])
                if row.get("session_key") != session_key
            ] + rows

        session_meta = raw["drivers"][0]
        existing_sessions[session_key] = {
            "session_key": session_key,
            "year": 2025,
            "name": race_name,
            "circuit": race_name.removeprefix("2025 ").removesuffix(" Grand Prix"),
            "date": RACE_DATES[session_key],
            "meeting_key": session_meta.get("meeting_key"),
        }

    dashboard["races"] = sorted(
        existing_sessions.values(), key=lambda race: race.get("date", "")
    )
    write_dashboard_data(dashboard)
    spark.stop()
    print(f"Wrote {len(dashboard['races'])} races to {len(DATA_FILES)} dashboard data files")


if __name__ == "__main__":
    main()
