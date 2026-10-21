from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from scripts.refresh_dashboard_data import (
    DATA_FILES,
    fetch_from_bigquery,
    main,
    write_dashboard_data,
)

ROOT = Path(__file__).resolve().parents[1]


class FakeRow:
    """Mock BigQuery Row."""

    def __init__(self, data: dict):
        self._data = data

    def items(self):
        return self._data.items()

    def __getitem__(self, item):
        return self._data[item]


def test_fetch_from_bigquery_schema_and_races_sorting():
    """Verify that fetch_from_bigquery builds the exact schema required by app.js with sorted races."""
    mock_bq = MagicMock()

    driver_summary_rows = [
        FakeRow({
            "session_key": 9472,
            "meeting_key": 1229,
            "driver_number": 1,
            "driver_name": "Max Verstappen",
            "name_acronym": "VER",
            "team_name": "Red Bull Racing",
            "team_colour": "3671c6",
            "total_laps": 57,
            "fastest_lap_seconds": 92.6,
            "avg_lap_seconds": 95.8,
            "total_pit_stops": 2,
            "total_pit_duration": 49.2,
            "starting_compound": "SOFT",
            "max_stint_length": 20,
        })
    ]
    tyre_deg_rows = [
        FakeRow({
            "session_key": 9472,
            "driver_number": 1,
            "driver_name": "Max Verstappen",
            "team_name": "Red Bull Racing",
            "stint_number": 1,
            "compound": "SOFT",
            "tyre_age": 5,
            "lap_number": 5,
            "lap_time_seconds": 95.5,
            "stint_avg_lap_time": 96.0,
            "delta_to_stint_avg": -0.5,
        })
    ]
    pit_strat_rows = [
        FakeRow({
            "session_key": 9472,
            "driver_number": 1,
            "driver_name": "Max Verstappen",
            "team_name": "Red Bull Racing",
            "stint_number": 1,
            "compound": "SOFT",
            "lap_start": 1,
            "lap_end": 20,
            "stint_length": 20,
            "pit_lap": 20,
            "pit_duration": 24.5,
            "stop_duration": 2.5,
        })
    ]
    weather_rows = [
        FakeRow({
            "session_key": 9472,
            "date_utc": "2024-03-02 15:00:00",
            "air_temperature": 25.0,
            "track_temperature": 32.0,
            "humidity": 45.0,
            "pressure": 1012.0,
            "wind_speed": 2.5,
            "wind_direction": 180,
            "rainfall": 0,
            "field_avg_lap_time": 95.5,
        })
    ]
    dim_sessions_rows = [
        FakeRow({
            "session_key": 9472,
            "meeting_key": 1229,
            "year": 2024,
            "session_name": "2024 Bahrain Grand Prix",
            "circuit_short_name": "Sakhir",
            "country_name": "Bahrain",
            "date_start": "2024-03-02 15:00:00+00:00",
        }),
        FakeRow({
            "session_key": 9693,
            "meeting_key": 1254,
            "year": 2025,
            "session_name": "2025 Australian Grand Prix",
            "circuit_short_name": "Albert Park",
            "country_name": "Australia",
            "date_start": "2025-03-16 04:00:00+00:00",
        }),
    ]

    race_results_rows = [
        FakeRow({
            "session_key": 9472,
            "driver_number": 1,
            "driver_name": "Max Verstappen",
            "name_acronym": "VER",
            "team_name": "Red Bull Racing",
            "team_colour": "3671c6",
            "grid_position": 1,
            "final_position": 1,
            "positions_gained": 0,
            "status": "Finished",
            "total_pit_stops": 2,
            "number_of_laps": 57,
            "points": 26.0,
            "duration_seconds": 5490.5,
            "gap_to_leader_seconds": 0.0,
            "laps_down": None,
            "grid_position_missing": False,
        }),
        FakeRow({
            "session_key": 9472,
            "driver_number": 2,
            "driver_name": "Logan Sargeant",
            "name_acronym": "SAR",
            "team_name": "Williams",
            "team_colour": "00a0de",
            "grid_position": 18,
            "final_position": None,
            "positions_gained": None,
            "status": "DNF",
            "total_pit_stops": 1,
            "number_of_laps": 30,
            "points": 0.0,
            "duration_seconds": None,
            "gap_to_leader_seconds": None,
            "laps_down": None,
            "grid_position_missing": False,
        }),
        FakeRow({
            "session_key": 9472,
            "driver_number": 31,
            "driver_name": "Esteban Ocon",
            "name_acronym": "OCO",
            "team_name": "Alpine",
            "team_colour": "0090ff",
            "grid_position": None,
            "final_position": 17,
            "positions_gained": None,
            "status": "Finished",
            "total_pit_stops": 2,
            "number_of_laps": 56,
            "points": 0.0,
            "duration_seconds": None,
            "gap_to_leader_seconds": None,
            "laps_down": 1,
            "grid_position_missing": True,
        }),
    ]

    def query_side_effect(sql):
        mock_result = MagicMock()
        if "agg_driver_race_summary" in sql:
            mock_result.result.return_value = driver_summary_rows
        elif "agg_tyre_degradation" in sql:
            mock_result.result.return_value = tyre_deg_rows
        elif "agg_pit_strategy" in sql:
            mock_result.result.return_value = pit_strat_rows
        elif "agg_weather_impact" in sql:
            mock_result.result.return_value = weather_rows
        elif "agg_race_results" in sql:
            mock_result.result.return_value = race_results_rows
        elif "dim_sessions" in sql:
            mock_result.result.return_value = dim_sessions_rows
        else:
            mock_result.result.return_value = []
        return mock_result

    mock_bq.query.side_effect = query_side_effect

    # Include session 10014 (not in dim_sessions rows) to verify fallback
    session_keys = [9693, 9472, 10014]
    dashboard = fetch_from_bigquery(mock_bq, "test-project", session_keys)

    # 1. Assert required top-level keys
    assert set(dashboard.keys()) == {
        "driver_summary",
        "tyre_degradation",
        "pit_strategy",
        "weather_impact",
        "race_results",
        "races",
    }

    # 2. Assert schema keys match what app.js consumes
    assert len(dashboard["driver_summary"]) == 1
    ds = dashboard["driver_summary"][0]
    for key in ["driver_name", "team_name", "total_laps", "fastest_lap_seconds", "avg_lap_seconds", "total_pit_stops"]:
        assert key in ds

    assert len(dashboard["tyre_degradation"]) == 1
    td = dashboard["tyre_degradation"][0]
    for key in ["driver_number", "compound", "tyre_age", "lap_time_seconds", "stint_avg_lap_time", "delta_to_stint_avg"]:
        assert key in td

    assert len(dashboard["pit_strategy"]) == 1
    ps = dashboard["pit_strategy"][0]
    for key in ["driver_number", "stint_number", "compound", "lap_start", "lap_end", "pit_lap", "pit_duration"]:
        assert key in ps

    assert len(dashboard["weather_impact"]) == 1
    wi = dashboard["weather_impact"][0]
    for key in ["date_utc", "air_temperature", "track_temperature", "field_avg_lap_time"]:
        assert key in wi

    # Assert race_results schema and ordering contract
    assert len(dashboard["race_results"]) == 3
    rr = dashboard["race_results"]
    for r in rr:
        for key in [
            "final_position", "status", "grid_position", "positions_gained", "points",
            "number_of_laps", "laps_down", "gap_to_leader_seconds", "duration_seconds",
            "grid_position_missing"
        ]:
            assert key in r
    # Ordering: P1 first, P17 second, null-position DNF last
    assert rr[0]["final_position"] == 1
    assert rr[1]["final_position"] == 17
    assert rr[2]["final_position"] is None
    assert rr[2]["status"] == "DNF"

    # Assert session 9693 and 10014 (no race results) have NO rows in race_results
    assert all(r["session_key"] == 9472 for r in rr)

    # 3. Assert races list is sorted by date
    races = dashboard["races"]
    assert len(races) == 3
    assert races[0]["session_key"] == 9472
    assert races[0]["date"] == "2024-03-02"
    assert races[0]["circuit"] == "Sakhir"

    assert races[1]["session_key"] == 9693
    assert races[1]["date"] == "2025-03-16"
    assert races[1]["circuit"] == "Albert Park"

    # 4. Verify fallback for session 10014
    assert races[2]["session_key"] == 10014
    assert races[2]["date"] == "2025-04-13"
    assert races[2]["name"] == "2025 Bahrain Grand Prix"


def test_gold_data_files_stay_identical():
    """Verify that dashboard/gold_data.json and docs/gold_data.json stay identical."""
    dashboard_file = DATA_FILES[0]
    docs_file = DATA_FILES[1]

    assert dashboard_file.exists()
    assert docs_file.exists()
    assert dashboard_file.read_text(encoding="utf-8") == docs_file.read_text(encoding="utf-8")


def test_write_dashboard_data_writes_both_files_identically(tmp_path, monkeypatch):
    """Verify write_dashboard_data produces identical content across both files."""
    f1 = tmp_path / "dashboard" / "gold_data.json"
    f2 = tmp_path / "docs" / "gold_data.json"
    f1.parent.mkdir(parents=True, exist_ok=True)
    f2.parent.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr("scripts.refresh_dashboard_data.DATA_FILES", (f1, f2))

    data = {"test_key": [1, 2, 3], "races": [{"session_key": 9472, "date": "2024-03-02"}]}
    write_dashboard_data(data)

    assert f1.read_text(encoding="utf-8") == f2.read_text(encoding="utf-8")


def test_refresh_main_bigquery_mode(monkeypatch):
    """Verify --source bigquery invokes BigQuery fetch and write path."""
    mock_dashboard = {
        "driver_summary": [],
        "tyre_degradation": [],
        "pit_strategy": [],
        "weather_impact": [],
        "race_results": [],
        "races": [{"session_key": 9472, "date": "2024-03-02"}],
    }

    mock_fetch = MagicMock(return_value=mock_dashboard)
    mock_write = MagicMock()
    mock_bq_client_cls = MagicMock()

    monkeypatch.setattr("scripts.refresh_dashboard_data.fetch_from_bigquery", mock_fetch)
    monkeypatch.setattr("scripts.refresh_dashboard_data.write_dashboard_data", mock_write)
    monkeypatch.setattr("google.cloud.bigquery.Client", mock_bq_client_cls)
    monkeypatch.setenv("GCP_PROJECT_ID", "mock-gcp-project")

    main(["--source", "bigquery"])

    mock_fetch.assert_called_once()
    mock_write.assert_called_once_with(mock_dashboard)
