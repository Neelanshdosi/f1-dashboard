import json
import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = REPO_ROOT / "dashboard"
DOCS_DIR = REPO_ROOT / "docs"

# Single FIELDS definition for frontend contract verification
CONTRACT_FIELDS = {
    "race_results": [
        "session_key",
        "driver_number",
        "driver_name",
        "name_acronym",
        "team_name",
        "team_colour",
        "final_position",
        "grid_position",
        "positions_gained",
        "status",
        "total_pit_stops",
        "duration_seconds",
        "gap_to_leader_seconds",
        "laps_down",
        "number_of_laps",
        "points",
    ],
    "races": [
        "session_key",
        "meeting_key",
        "name",
        "circuit",
        "date",
        "year",
        "has_results",
        "threshold_stops_count",
        "stop_detection_summary",
    ],
    "meta": [
        "races_with_results",
        "races_with_grid",
    ],
}


def test_fields_contract_matches_gold_data():
    """Verify that dashboard/gold_data.json matches the CONTRACT_FIELDS definition."""
    gold_data_path = DASHBOARD_DIR / "gold_data.json"
    assert gold_data_path.exists(), "gold_data.json must exist in dashboard/"

    with open(gold_data_path, encoding="utf-8") as f:
        data = json.load(f)

    # Check top-level required keys
    for table_name in ["meta", "races", "race_results"]:
        assert table_name in data, f"Top-level key {table_name} missing from gold_data.json"

    # Meta keys check
    for field in CONTRACT_FIELDS["meta"]:
        assert field in data["meta"], f"Missing meta field: {field}"

    # Races keys check
    assert len(data["races"]) > 0, "races list should not be empty"
    for field in CONTRACT_FIELDS["races"]:
        for race in data["races"]:
            assert field in race, f"Missing race field: {field} in session {race.get('session_key')}"

    # Race results keys check
    assert len(data["race_results"]) > 0, "race_results list should not be empty"
    for field in CONTRACT_FIELDS["race_results"]:
        for row in data["race_results"]:
            assert field in row, f"Missing race_results field: {field} in session {row.get('session_key')}"


def test_dashboard_and_docs_are_byte_identical():
    """Verify dashboard/ and docs/ static frontend files are strictly byte-identical."""
    files_to_check = ["index.html", "app.js", "style.css", "gold_data.json"]
    for filename in files_to_check:
        dash_file = DASHBOARD_DIR / filename
        docs_file = DOCS_DIR / filename
        assert dash_file.exists(), f"dashboard/{filename} missing"
        assert docs_file.exists(), f"docs/{filename} missing"
        assert dash_file.read_bytes() == docs_file.read_bytes(), (
            f"dashboard/{filename} and docs/{filename} are not byte-identical"
        )


def test_no_hardcoded_driver_or_race_names_in_app_js():
    """Verify app.js contains no hardcoded driver names or race names."""
    app_js_path = DASHBOARD_DIR / "app.js"
    content = app_js_path.read_text(encoding="utf-8")

    forbidden_patterns = [
        r"\bVerstappen\b",
        r"\bNorris\b",
        r"\bHamilton\b",
        r"\bLeclerc\b",
        r"\bPiastri\b",
        r"\bSainz\b",
        r"\bRussell\b",
        r"\bAlonso\b",
        r"\bBottas\b",
        r"\bHulkenberg\b",
        r"\bAbu Dhabi\b",
        r"\bBahrain\b",
        r"\bAustralia\b",
        r"\bMonaco\b",
        r"\bBritish\b",
    ]

    for pattern in forbidden_patterns:
        match = re.search(pattern, content, flags=re.IGNORECASE)
        assert match is None, f"Found hardcoded string matching {pattern} in app.js"


def _run_node_expression(expr: str) -> str:
    """Helper to run a JS expression through Node via stdin against pure functions in app.js."""
    app_js_path = DASHBOARD_DIR / "app.js"
    js_code = (
        f"{app_js_path.read_text(encoding='utf-8')}\n"
        f"console.log(JSON.stringify({expr}));\n"
    )
    result = subprocess.run(
        ["node"],
        input=js_code,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=True,
    )
    return json.loads(result.stdout.strip())



def test_format_helpers_winner_time():
    """Verify formatTimeGap for winner (final_position = 1) returns formatted race duration."""
    winner_row = {"final_position": 1, "duration_seconds": 5422.384, "gap_to_leader_seconds": 0.0}
    res = _run_node_expression(f"formatTimeGap({json.dumps(winner_row)})")
    assert res == "1:30:22.384"

    sub_hour_row = {"final_position": 1, "duration_seconds": 1845.123}
    assert _run_node_expression(f"formatTimeGap({json.dumps(sub_hour_row)})") == "30:45.123"


def test_format_helpers_lead_lap_gap():
    """Verify formatTimeGap for lead-lap driver returns +X.XXX s."""
    p2_row = {"final_position": 2, "gap_to_leader_seconds": 22.457, "laps_down": 0}
    res = _run_node_expression(f"formatTimeGap({json.dumps(p2_row)})")
    assert res == "+22.457 s"


def test_format_helpers_plus_one_lap():
    """Verify formatTimeGap returns '+1 LAP' and formatLapsDown pluralizes correctly."""
    p15_row = {"final_position": 15, "gap_to_leader_seconds": None, "laps_down": 1}
    assert _run_node_expression(f"formatTimeGap({json.dumps(p15_row)})") == "+1 LAP"

    p18_row = {"final_position": 18, "gap_to_leader_seconds": None, "laps_down": 2}
    assert _run_node_expression(f"formatTimeGap({json.dumps(p18_row)})") == "+2 LAPS"


def test_format_helpers_classified_dnf_and_null_position():
    """Verify formatStatusBadge and formatFinalPosition handles null/DNF."""
    assert _run_node_expression("formatFinalPosition(null)") == "—"
    assert _run_node_expression("formatFinalPosition(3)") == "3"
    assert _run_node_expression("formatGridPosition(null)") == "—"
    assert _run_node_expression("formatGridPosition(12)") == "12"

    dnf_badge = _run_node_expression("formatStatusBadge('DNF', null)")
    assert 'class="status-badge dnf"' in dnf_badge
    assert "DNF" in dnf_badge


def test_format_helpers_zero_gained():
    """Verify formatPositionsGained returns '0' and no change label when gained is 0."""
    res_zero = _run_node_expression("formatPositionsGained(0)")
    assert res_zero["text"] == "0"
    assert res_zero["label"] == "no change in position"
    assert 'class="pos-same"' in res_zero["html"]

    res_pos = _run_node_expression("formatPositionsGained(3)")
    assert res_pos["text"] == "▲ 3"
    assert "gained 3 positions" in res_pos["label"]

    res_neg = _run_node_expression("formatPositionsGained(-2)")
    assert res_neg["text"] == "▼ 2"
    assert "lost 2 positions" in res_neg["label"]

    res_null = _run_node_expression("formatPositionsGained(null)")
    assert res_null["text"] == "—"
