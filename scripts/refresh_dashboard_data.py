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
ENDPOINTS = ("drivers", "laps", "stints", "pit", "weather")

GOLD_TABLE_MAP = {
    "agg_driver_race_summary": "driver_summary",
    "agg_tyre_degradation": "tyre_degradation",
    "agg_pit_strategy": "pit_strategy",
    "agg_weather_impact": "weather_impact",
    "agg_race_results": "race_results",
}


def sort_race_results(records: list[dict]) -> list[dict]:
    """Ordering contract for race_results:

    1. final_position ascending with nulls last
    2. Among null positions, order status: DNF (0), DSQ (1), DNS (2), Unclassified (3), other (4)
    3. Tie-break by driver_number ascending
    """
    status_order = {"DNF": 0, "DSQ": 1, "DNS": 2, "Unclassified": 3}

    def sort_key(row):
        pos = row.get("final_position")
        has_pos = 0 if (pos is not None) else 1
        pos_val = pos if pos is not None else 0
        stat = row.get("status", "")
        stat_rank = status_order.get(stat, 4)
        drv = row.get("driver_number") or 0
        return (has_pos, pos_val, stat_rank, drv)

    return sorted(records, key=sort_key)


def _load_dashboard_config() -> tuple[str | None, list[int]]:
    import yaml

    config_path = ROOT / "config.yaml"
    project_id = os.getenv("GCP_PROJECT_ID")
    session_keys = [9472, 9693, 10014, 9979, 9947, 9839]

    if config_path.exists():
        with config_path.open(encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
            if not project_id:
                raw_project = cfg.get("gcp", {}).get("project_id", "")
                if raw_project and not raw_project.startswith("${"):
                    project_id = raw_project
            dashboard_cfg = cfg.get("dashboard", {})
            if "session_keys" in dashboard_cfg and dashboard_cfg["session_keys"]:
                session_keys = dashboard_cfg["session_keys"]

    return project_id, session_keys


def fetch_from_bigquery(bq_client, project_id: str, session_keys: list[int]) -> dict:
    """Fetch Gold tables and dim_sessions from BigQuery for the given session keys."""
    dashboard: dict[str, list] = {
        "driver_summary": [],
        "tyre_degradation": [],
        "pit_strategy": [],
        "weather_impact": [],
        "race_results": [],
        "races": [],
    }

    if not session_keys:
        return dashboard

    keys_csv = ", ".join(str(k) for k in session_keys)

    # 1. Query the Gold tables (with session_key filter for partition pruning)
    for bq_table, json_key in GOLD_TABLE_MAP.items():
        query = f"""
            SELECT *
            FROM `{project_id}.f1_analytics.{bq_table}`
            WHERE session_key IN ({keys_csv})
        """
        rows = bq_client.query(query).result()
        table_records = []
        for row in rows:
            record = dict(row.items())
            for k, v in record.items():
                if hasattr(v, "isoformat"):
                    record[k] = str(v)
            table_records.append(record)

        if json_key == "race_results":
            # Apply ordering contract and ensure a race with no rows is omitted entirely
            # Group by session_key and sort each session's results
            sessions_with_results = {r.get("session_key") for r in table_records if r.get("session_key") is not None}
            sorted_records = []
            for sk in session_keys:
                if sk in sessions_with_results:
                    session_rows = [r for r in table_records if r.get("session_key") == sk]
                    sorted_records.extend(sort_race_results(session_rows))
            dashboard[json_key] = sorted_records
        else:
            dashboard[json_key] = table_records

    # 2. Build races list from dim_sessions
    sessions_by_key = {}
    try:
        dim_query = f"""
            SELECT session_key, meeting_key, year, session_name, meeting_name, circuit_short_name, country_name, date_start
            FROM `{project_id}.f1_analytics.dim_sessions`
            WHERE session_key IN ({keys_csv})
        """
        dim_rows = bq_client.query(dim_query).result()
        for r in dim_rows:
            sessions_by_key[r["session_key"]] = dict(r.items())
    except Exception as e:
        print(f"  [WARN] Could not query dim_sessions ({e}), falling back to defaults")

    races = []
    for sk in session_keys:
        meta = sessions_by_key.get(sk, {})
        year = meta.get("year")
        if not year:
            year = 2024 if sk == 9472 else 2025

        meeting_name = meta.get("meeting_name")
        country_name = meta.get("country_name")

        if meeting_name:
            name = f"{year} {meeting_name}"
        elif country_name:
            name = f"{year} {country_name} Grand Prix"
        elif meta.get("session_name"):
            name = meta["session_name"]
        else:
            name = f"{year} Race {sk}"

        circuit = meta["circuit_short_name"] if meta.get("circuit_short_name") else "Circuit"

        date_val = meta.get("date_start")
        date_str = str(date_val)[:10] if date_val is not None else ""

        race_entry = {
            "session_key": sk,
            "year": year,
            "name": name,
            "circuit": circuit,
            "date": date_str,
            "has_results": sk in sessions_with_results,
        }
        if meta.get("meeting_key") is not None:
            race_entry["meeting_key"] = meta["meeting_key"]

        races.append(race_entry)

    # Sort races by date
    races.sort(key=lambda r: (r.get("year", 0), r.get("date", "")))
    dashboard["races"] = races

    races_with_results = [r["session_key"] for r in races if r.get("has_results")]
    races_with_grid = sorted(
        {
            r["session_key"]
            for r in dashboard.get("race_results", [])
            if r.get("grid_position") is not None
        }
    )
    dashboard["meta"] = {
        "races_with_results": races_with_results,
        "races_with_grid": races_with_grid,
    }

    return dashboard


def fetch(
    session: requests.Session,
    endpoint: str,
    session_key: int | None = None,
    **extra_params,
) -> list[dict]:
    # OpenF1 allows 30 requests/minute. Keep this refresh at or below that rate.
    time.sleep(2.1)
    url = f"{API}/{endpoint}"
    params = {}
    if session_key is not None:
        params["session_key"] = session_key
    params.update(extra_params)

    response = session.get(url, params=params, timeout=90)

    if response.status_code == 200:
        data = response.json()
        return data if isinstance(data, list) else [data]

    if response.status_code == 404:
        try:
            err_json = response.json()
        except Exception:
            err_json = None

        if isinstance(err_json, dict) and err_json.get("detail") == "No results found.":
            return []

        raise requests.HTTPError(
            f"HTTP 404 Not Found for {url} with params {params}: {response.text}",
            response=response,
        )

    response.raise_for_status()
    return response.json()


def build_race_entry_from_api(
    session: requests.Session,
    session_key: int,
    has_results: bool = False,
) -> dict:
    """Build race entry dict from OpenF1 /v1/sessions and /v1/meetings."""
    sessions_data = fetch(session, "sessions", session_key=session_key)
    if not sessions_data:
        raise RuntimeError(f"OpenF1 sessions endpoint returned no data for session_key={session_key}")

    sess = sessions_data[0]
    year = sess.get("year") or 2025
    meeting_key = sess.get("meeting_key")
    country_name = sess.get("country_name")
    circuit_short_name = sess.get("circuit_short_name")
    date_start = sess.get("date_start")

    meeting_name = None
    if meeting_key is not None:
        try:
            meetings_data = fetch(session, "meetings", meeting_key=meeting_key)
            if meetings_data:
                meeting_name = meetings_data[0].get("meeting_name")
        except Exception as e:
            print(f"  [WARN] Failed to fetch meeting metadata for meeting_key={meeting_key}: {e}")

    if meeting_name:
        name = f"{year} {meeting_name}"
    elif country_name:
        name = f"{year} {country_name} Grand Prix"
    else:
        name = f"{year} Race {session_key}"

    circuit = circuit_short_name or "Circuit"
    date_str = str(date_start)[:10] if date_start else ""

    entry = {
        "session_key": session_key,
        "year": year,
        "name": name,
        "circuit": circuit,
        "date": date_str,
        "has_results": has_results,
    }
    if meeting_key is not None:
        entry["meeting_key"] = meeting_key
    return entry


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
    import tempfile

    content = json.dumps(dashboard, indent=2, default=str) + "\n"
    for path in DATA_FILES:
        path_obj = Path(path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)
        # Atomic write: write to temp file in same directory, then replace
        with tempfile.NamedTemporaryFile("w", dir=path_obj.parent, delete=False, encoding="utf-8") as tf:
            tf.write(content)
            temp_name = tf.name
        Path(temp_name).replace(path_obj)


def main(argv: list[str] | None = None) -> None:
    import argparse
    import logging

    parser = argparse.ArgumentParser(description="Refresh static F1 dashboard data")
    parser.add_argument(
        "--source",
        choices=["api", "bigquery"],
        default="api",
        help="Data source: 'api' (OpenF1 API via Spark) or 'bigquery' (Gold BigQuery tables)",
    )
    args = parser.parse_args(argv)

    if args.source == "bigquery":
        from google.cloud import bigquery

        project_id, session_keys = _load_dashboard_config()
        if not project_id:
            raise ValueError(
                "GCP_PROJECT_ID environment variable or gcp.project_id in config.yaml must be set"
            )

        print(f"Refreshing dashboard from BigQuery ({project_id}) for sessions: {session_keys}...")
        bq_client = bigquery.Client(project=project_id)
        dashboard = fetch_from_bigquery(bq_client, project_id, session_keys)
        write_dashboard_data(dashboard)
        print(f"Wrote {len(dashboard['races'])} races to {len(DATA_FILES)} dashboard data files from BigQuery")
        return

    from spark_jobs.bronze_to_silver import (
        transform_dim_drivers,
        transform_fct_laps,
        transform_fct_pit,
        transform_fct_session_result,
        transform_fct_starting_grid,
        transform_fct_stints,
        transform_fct_weather,
    )
    from spark_jobs.silver_to_gold import (
        compute_driver_race_summary,
        compute_pit_strategy,
        compute_race_results,
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

    _project_id, session_keys = _load_dashboard_config()
    existing_sessions = {
        race["session_key"]: race for race in dashboard.get("races", [])
    }

    all_race_results: list[dict] = [
        row
        for row in dashboard.get("race_results", [])
        if row.get("session_key") not in session_keys
    ]

    for session_key in session_keys:
        race_entry = build_race_entry_from_api(http, session_key, has_results=False)
        race_name = race_entry["name"]
        print(f"Fetching {race_name} ({session_key})", flush=True)
        raw = {endpoint: fetch(http, endpoint, session_key) for endpoint in ENDPOINTS}
        if not raw["drivers"] or not raw["laps"] or not raw["stints"]:
            raise RuntimeError(f"OpenF1 returned incomplete race data for {race_name}")

        from ingestion.openf1_client import resolve_qualifying_session

        raw_session_result = fetch(http, "session_result", session_key)
        qual_key = resolve_qualifying_session(http, session_key)
        if qual_key is not None:
            raw_starting_grid_qual = fetch(http, "starting_grid", qual_key)
            raw_starting_grid = []
            for row in raw_starting_grid_qual:
                row_copy = dict(row)
                row_copy["session_key"] = session_key
                row_copy["grid_source_session_key"] = qual_key
                raw_starting_grid.append(row_copy)
        else:
            raw_starting_grid = []

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

        # Process race_results using Silver transforms and compute_race_results
        has_results = False
        if not raw_session_result:
            logging.error(
                "OpenF1 session_result is missing or empty for %s (session_key=%s). Omission from race_results.",
                race_name,
                session_key,
            )
        else:
            has_results = True
            for r in raw_session_result:
                for k in ["duration", "gap_to_leader"]:
                    if k in r and r[k] is not None:
                        r[k] = json.dumps(r[k]) if isinstance(r[k], (list, dict)) else str(r[k])

            sr_df = transform_fct_session_result(spark_frame(spark, raw_session_result))
            sg_df = (
                transform_fct_starting_grid(spark_frame(spark, raw_starting_grid))
                if raw_starting_grid
                else None
            )

            results_df = compute_race_results(sr_df, sg_df, drivers, stints, pit)
            results_records = as_records(results_df)
            sorted_results = sort_race_results(results_records)
            all_race_results.extend(sorted_results)

        race_entry["has_results"] = has_results
        existing_sessions[session_key] = race_entry

    dashboard["race_results"] = all_race_results
    races_list = sorted(
        existing_sessions.values(), key=lambda race: race.get("date", "")
    )
    dashboard["races"] = races_list

    races_with_results = [r["session_key"] for r in races_list if r.get("has_results")]
    races_with_grid = sorted(
        {
                r["session_key"]
                for r in dashboard.get("race_results", [])
                if r.get("grid_position") is not None
            }
    )
    existing_meta = dashboard.get("meta", {})
    existing_meta["races_with_results"] = races_with_results
    existing_meta["races_with_grid"] = races_with_grid
    dashboard["meta"] = existing_meta

    write_dashboard_data(dashboard)
    spark.stop()
    print(f"Wrote {len(dashboard['races'])} races to {len(DATA_FILES)} dashboard data files")


if __name__ == "__main__":
    main()
