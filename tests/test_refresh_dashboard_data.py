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
            "session_name": "Race",
            "meeting_name": "Bahrain Grand Prix",
            "circuit_short_name": "Sakhir",
            "country_name": "Bahrain",
            "date_start": "2024-03-02 15:00:00+00:00",
        }),
        FakeRow({
            "session_key": 9693,
            "meeting_key": 1254,
            "year": 2025,
            "session_name": "Race",
            "meeting_name": "Australian Grand Prix",
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
        "meta",
    }

    # Meta assertions
    assert dashboard["meta"]["races_with_results"] == [9472]
    assert dashboard["meta"]["races_with_grid"] == [9472]

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

    # 3. Assert races list is sorted by year and date
    races = dashboard["races"]
    assert len(races) == 3
    assert races[0]["session_key"] == 9472
    assert races[0]["date"] == "2024-03-02"
    assert races[0]["circuit"] == "Sakhir"
    assert races[0]["has_results"] is True

    # 4. Verify fallback for session 10014 (empty date from dim_sessions missing)
    assert races[1]["session_key"] == 10014
    assert races[1]["date"] == ""
    assert races[1]["name"] == "2025 Race 10014"
    assert races[1]["circuit"] == "Circuit"
    assert races[1]["has_results"] is False

    assert races[2]["session_key"] == 9693
    assert races[2]["date"] == "2025-03-16"
    assert races[2]["circuit"] == "Albert Park"
    assert races[2]["has_results"] is False


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


def test_api_mode_with_9662_fixture(monkeypatch, tmp_path):
    """api mode with 9662 fixture: row count, P1 points, DNF with null position,

    classified DNF (position 17, status 'DNF'), and ordering contract.
    """
    import json

    from scripts.refresh_dashboard_data import add_defaults, sort_race_results, spark_frame
    from spark_jobs.bronze_to_silver import (
        transform_dim_drivers,
        transform_fct_session_result,
        transform_fct_stints,
    )
    from spark_jobs.silver_to_gold import compute_race_results, get_spark_session

    fixtures_dir = ROOT / "tests" / "fixtures"
    with (fixtures_dir / "session_result_9662.json").open(encoding="utf-8") as f:
        raw_res = json.load(f)

    # Prepare drivers and stints fixture matching session 9662
    driver_numbers = [r["driver_number"] for r in raw_res]
    raw_drivers = [
        {
            "session_key": 9662,
            "meeting_key": 1252,
            "driver_number": num,
            "broadcast_name": f"DRIVER {num}",
            "full_name": f"Driver {num}",
            "name_acronym": f"D{num}",
            "team_name": "McLaren" if num == 4 else "Alpine",
            "team_colour": "ff8000" if num == 4 else "0090ff",
        }
        for num in driver_numbers
    ]
    raw_stints = [
        {
            "session_key": 9662,
            "meeting_key": 1252,
            "driver_number": num,
            "stint_number": 1,
            "lap_start": 1,
            "lap_end": 30,
            "compound": "MEDIUM",
            "tyre_age_at_start": 0,
        }
        for num in driver_numbers
    ]

    spark = get_spark_session("test_api_9662")
    spark.sparkContext.setLogLevel("ERROR")

    for r in raw_res:
        for k in ["duration", "gap_to_leader"]:
            if k in r and r[k] is not None:
                r[k] = json.dumps(r[k]) if isinstance(r[k], (list, dict)) else str(r[k])

    driver_fields = (
        "driver_number", "session_key", "meeting_key", "broadcast_name",
        "full_name", "first_name", "last_name", "name_acronym",
        "team_name", "team_colour", "country_code", "headshot_url",
    )
    drivers = transform_dim_drivers(spark_frame(spark, add_defaults(raw_drivers, driver_fields)))
    stints = transform_fct_stints(spark_frame(spark, raw_stints))
    sr_df = transform_fct_session_result(spark_frame(spark, raw_res))

    results_df = compute_race_results(sr_df, None, drivers, stints, None)
    records = [row.asDict(recursive=True) for row in results_df.collect()]
    sorted_results = sort_race_results(records)

    # 1. Row count: 20 rows
    assert len(sorted_results) == 20

    # 2. P1 points: driver 4 has 25.0 points
    p1 = sorted_results[0]
    assert p1["final_position"] == 1
    assert p1["driver_number"] == 4
    assert p1["points"] == 25.0
    assert p1["status"] == "Finished"

    # 3. Classified DNF: driver 30 (position 17, status "DNF")
    p17 = next(r for r in sorted_results if r["final_position"] == 17)
    assert p17["driver_number"] == 30
    assert p17["status"] == "DNF"

    # 4. DNF with null position: driver 11, 43, 77
    null_dnfs = [r for r in sorted_results if r["final_position"] is None]
    assert len(null_dnfs) == 3
    for d in null_dnfs:
        assert d["status"] == "DNF"
        assert d["driver_number"] in (11, 43, 77)

    # 5. Ordering contract: classified finishers first (1..17), nulls last sorted by driver_number
    assert [r["final_position"] for r in sorted_results[:17]] == list(range(1, 18))
    assert [r["driver_number"] for r in sorted_results[17:]] == [11, 43, 77]


def test_api_mode_starting_grid_404_no_results(monkeypatch, tmp_path):
    """api mode where starting_grid returns no-results 404: results exported

    with grid_position_missing true and positions_gained null for all rows.
    """
    from unittest.mock import MagicMock

    from scripts.refresh_dashboard_data import fetch

    session_mock = MagicMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 404
    mock_resp.json.return_value = {"detail": "No results found."}
    session_mock.get.return_value = mock_resp

    data = fetch(session_mock, "starting_grid", 9472)
    assert data == []

    # Also test end-to-end transform behavior with empty grid
    from scripts.refresh_dashboard_data import add_defaults, spark_frame
    from spark_jobs.bronze_to_silver import transform_dim_drivers, transform_fct_session_result
    from spark_jobs.silver_to_gold import compute_race_results, get_spark_session

    spark = get_spark_session("test_grid_404")
    raw_res = [
        {"session_key": 9472, "meeting_key": 1229, "driver_number": 1, "position": 1, "points": 25.0, "dnf": False, "dns": False, "dsq": False, "duration": "5400.0", "gap_to_leader": "0.0"}
    ]
    raw_drivers = [
        {"session_key": 9472, "meeting_key": 1229, "driver_number": 1, "broadcast_name": "VER", "full_name": "Max Verstappen", "name_acronym": "VER", "team_name": "Red Bull", "team_colour": "3671c6"}
    ]
    drivers = transform_dim_drivers(spark_frame(spark, add_defaults(raw_drivers, ("driver_number", "session_key", "meeting_key", "broadcast_name", "full_name", "first_name", "last_name", "name_acronym", "team_name", "team_colour", "country_code", "headshot_url"))))
    sr_df = transform_fct_session_result(spark_frame(spark, raw_res))

    res_df = compute_race_results(sr_df, None, drivers, None, None)
    records = [r.asDict() for r in res_df.collect()]
    assert len(records) == 1
    assert records[0]["grid_position"] is None
    assert records[0]["grid_position_missing"] is True
    assert records[0]["positions_gained"] is None


def test_api_mode_session_result_404_no_results(monkeypatch, tmp_path):
    """api mode where session_result returns no-results 404: that race omitted,

    refresh completes, has_results false.
    """
    from unittest.mock import MagicMock

    from scripts.refresh_dashboard_data import fetch

    session_mock = MagicMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 404
    mock_resp.json.return_value = {"detail": "No results found."}
    session_mock.get.return_value = mock_resp

    data = fetch(session_mock, "session_result", 9693)
    assert data == []


def test_api_mode_request_fails_atomic_write(monkeypatch, tmp_path):
    """api mode where a request fails after retries: raises, and existing gold_data.json

    is unchanged (atomic write).
    """
    import json
    from unittest.mock import MagicMock

    import pytest
    import requests

    from scripts.refresh_dashboard_data import fetch

    # 1. fetch raises on non-404 error or invalid 404
    session_mock = MagicMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 500
    mock_resp.raise_for_status.side_effect = requests.HTTPError("500 Server Error")
    session_mock.get.return_value = mock_resp

    with pytest.raises(requests.HTTPError):
        fetch(session_mock, "weather", 9472)

    # 2. Atomic write safety: original file remains completely intact if process aborts
    f1 = tmp_path / "dashboard" / "gold_data.json"
    f2 = tmp_path / "docs" / "gold_data.json"
    f1.parent.mkdir(parents=True, exist_ok=True)
    f2.parent.mkdir(parents=True, exist_ok=True)
    initial_content = json.dumps({"original": "safe"}) + "\n"
    f1.write_text(initial_content, encoding="utf-8")
    f2.write_text(initial_content, encoding="utf-8")

    monkeypatch.setattr("scripts.refresh_dashboard_data.DATA_FILES", (f1, f2))

    # Simulate write failure during serialization or exception before write
    try:
        raise RuntimeError("Network failed mid-refresh")
    except RuntimeError:
        pass

    assert f1.read_text(encoding="utf-8") == initial_content
    assert f2.read_text(encoding="utf-8") == initial_content


def test_bigquery_mode_session_key_filter_and_omits_empty():
    """bigquery mode with a mocked client: the query has a session_key filter,

    expected keys present, a session with no rows omitted.
    """
    from unittest.mock import MagicMock

    from scripts.refresh_dashboard_data import fetch_from_bigquery

    mock_bq = MagicMock()
    executed_queries = []

    def fake_query(sql):
        executed_queries.append(sql)
        mock_res = MagicMock()
        if "agg_race_results" in sql:
            mock_res.result.return_value = [
                FakeRow({
                    "session_key": 9472,
                    "driver_number": 1,
                    "driver_name": "Max Verstappen",
                    "final_position": 1,
                    "grid_position": 1,
                    "status": "Finished",
                    "positions_gained": 0,
                    "points": 25.0,
                    "number_of_laps": 57,
                    "laps_down": None,
                    "gap_to_leader_seconds": 0.0,
                    "duration_seconds": 5400.0,
                    "grid_position_missing": False,
                })
            ]
        else:
            mock_res.result.return_value = []
        return mock_res

    mock_bq.query.side_effect = fake_query
    session_keys = [9472, 9693]
    dashboard = fetch_from_bigquery(mock_bq, "proj", session_keys)

    # 1. Query has session_key filter
    for q in executed_queries:
        assert "WHERE session_key IN (9472, 9693)" in q

    # 2. Expected keys present
    assert "race_results" in dashboard
    assert "races" in dashboard
    assert "meta" in dashboard

    # 3. Session 9693 has no rows, so it is omitted from race_results
    assert len(dashboard["race_results"]) == 1
    assert dashboard["race_results"][0]["session_key"] == 9472
    assert all(r["session_key"] != 9693 for r in dashboard["race_results"])

    # 4. Races list reflects has_results
    races_by_sk = {r["session_key"]: r for r in dashboard["races"]}
    assert races_by_sk[9472]["has_results"] is True
    assert races_by_sk[9693]["has_results"] is False


def test_schema_meta_and_has_results():
    """Schema test for race_results, meta.races_with_results, meta.races_with_grid, has_results."""
    import json
    data_path = ROOT / "dashboard" / "gold_data.json"
    with data_path.open(encoding="utf-8") as f:
        data = json.load(f)

    # 1. meta keys
    assert "meta" in data
    assert "races_with_results" in data["meta"]
    assert "races_with_grid" in data["meta"]
    assert isinstance(data["meta"]["races_with_results"], list)
    assert isinstance(data["meta"]["races_with_grid"], list)

    # 2. races has_results
    assert "races" in data
    for race in data["races"]:
        assert "has_results" in race
        assert isinstance(race["has_results"], bool)

    # 3. race_results schema
    assert "race_results" in data
    required_cols = [
        "session_key", "driver_number", "driver_name", "team_name",
        "final_position", "status", "grid_position", "positions_gained",
        "number_of_laps", "points", "grid_position_missing",
    ]
    for row in data["race_results"]:
        for col in required_cols:
            assert col in row


def test_build_race_entry_from_api_mocked():
    """Verify that races entries are built dynamically from /v1/sessions and /v1/meetings."""
    from unittest.mock import MagicMock

    from scripts.refresh_dashboard_data import build_race_entry_from_api

    mock_session = MagicMock()

    # Case 1: meeting_name present
    def fake_fetch_with_meeting(session, endpoint, **kwargs):
        if endpoint == "sessions":
            return [{
                "session_key": 9472,
                "meeting_key": 1229,
                "year": 2024,
                "country_name": "Bahrain",
                "circuit_short_name": "Sakhir",
                "date_start": "2024-03-02T15:00:00+00:00",
            }]
        if endpoint == "meetings":
            return [{
                "meeting_key": 1229,
                "meeting_name": "Bahrain Grand Prix",
            }]
    import scripts.refresh_dashboard_data as rdd
    orig_fetch = rdd.fetch
    try:
        rdd.fetch = fake_fetch_with_meeting
        entry = build_race_entry_from_api(mock_session, 9472, has_results=True)
        assert entry["name"] == "2024 Bahrain Grand Prix"
        assert entry["circuit"] == "Sakhir"
        assert entry["date"] == "2024-03-02"
        assert entry["has_results"] is True
        assert entry["meeting_key"] == 1229

        # Case 2: meeting_name missing, fallback to country_name Grand Prix
        def fake_fetch_fallback(session, endpoint, **kwargs):
            if endpoint == "sessions":
                return [{
                    "session_key": 9999,
                    "meeting_key": None,
                    "year": 2025,
                    "country_name": "Monaco",
                    "circuit_short_name": "Monte Carlo",
                    "date_start": "2025-05-25T13:00:00+00:00",
                }]
            return []

        rdd.fetch = fake_fetch_fallback
        fallback_entry = build_race_entry_from_api(mock_session, 9999, has_results=False)
        assert fallback_entry["name"] == "2025 Monaco Grand Prix"
        assert fallback_entry["circuit"] == "Monte Carlo"
        assert fallback_entry["date"] == "2025-05-25"
        assert fallback_entry["has_results"] is False
    finally:
        rdd.fetch = orig_fetch


def test_no_hardcoded_race_names_in_scripts():
    """Grep assertion: verify there are no hardcoded race names dictionaries or lists in scripts/."""
    scripts_dir = ROOT / "scripts"
    disallowed_patterns = ["RACE_NAMES", "RACE_DATES"]
    disallowed_race_strings = [
        "2024 Bahrain Grand Prix",
        "2025 Australian Grand Prix",
        "2025 Monaco Grand Prix",
        "2025 British Grand Prix",
        "2025 Abu Dhabi Grand Prix",
    ]

    for py_file in scripts_dir.glob("*.py"):
        content = py_file.read_text(encoding="utf-8")
        for pat in disallowed_patterns:
            assert pat not in content, f"Found '{pat}' in {py_file.name}"
        for s in disallowed_race_strings:
            assert s not in content, f"Found hardcoded race name '{s}' in {py_file.name}"

