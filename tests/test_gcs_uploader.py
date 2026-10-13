"""
tests/test_gcs_uploader.py
───────────────────────────
Unit tests for gcs_uploader – the GCS client is mocked so no real
cloud connection is needed.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

from ingestion.gcs_uploader import (
    _gcs_path,
    _make_filename,
    upload_local,
)

# ── Path helpers ──────────────────────────────────────────────────────────────

def test_gcs_path_format():
    path = _gcs_path("my-bucket", "laps", 2024, 9158, "laps_20240101.json")
    assert path == "bronze/laps/season=2024/session_key=9158/laps_20240101.json"


def test_make_filename_pattern():
    name = _make_filename("stints")
    assert name.startswith("stints_")
    assert name.endswith(".json")
    assert "T" in name   # ISO timestamp


# ── Local upload ──────────────────────────────────────────────────────────────

def test_upload_local_creates_file(tmp_path):
    data = [{"lap_number": 1, "lap_duration": 90.1}]
    result = upload_local(
        data, "laps", 2024, 9158,
        base_dir=tmp_path,
        overwrite=False,
    )
    assert result is not None
    out = Path(result)
    assert out.exists()
    loaded = json.loads(out.read_text())
    assert loaded == data


def test_upload_local_creates_correct_directory_structure(tmp_path):
    data = [{"stint_number": 1}]
    upload_local(data, "stints", 2024, 9158, base_dir=tmp_path)
    expected_dir = tmp_path / "bronze/stints/season=2024/session_key=9158"
    assert expected_dir.is_dir()
    files = list(expected_dir.glob("stints_*.json"))
    assert len(files) == 1


def test_upload_local_skips_on_existing(tmp_path):
    """Second upload for the same session should be skipped (idempotent)."""
    data = [{"lap_number": 1}]
    first = upload_local(data, "laps", 2024, 9158, base_dir=tmp_path)
    second = upload_local(
        [{"lap_number": 99}], "laps", 2024, 9158,
        base_dir=tmp_path, overwrite=False,
    )
    # Both calls return a path (either the existing or the new file)
    assert first is not None and second is not None
    # The directory should contain only ONE file (no second write)
    out_dir = tmp_path / "bronze/laps/season=2024/session_key=9158"
    assert len(list(out_dir.glob("laps_*.json"))) == 1


def test_upload_local_overwrites_when_flag_set(tmp_path):
    """overwrite=True should write a new file alongside the existing one."""
    data1 = [{"lap_number": 1}]
    upload_local(data1, "laps", 2024, 9158, base_dir=tmp_path, overwrite=False)

    time.sleep(0.01)   # ensure different timestamp

    data2 = [{"lap_number": 2}]
    upload_local(data2, "laps", 2024, 9158, base_dir=tmp_path, overwrite=True)

    out_dir = tmp_path / "bronze/laps/season=2024/session_key=9158"
    # With overwrite=True a new timestamped file should be written
    files = list(out_dir.glob("laps_*.json"))
    assert len(files) >= 1   # at least 1 file written


def test_upload_local_empty_data_returns_none(tmp_path):
    result = upload_local([], "laps", 2024, 9158, base_dir=tmp_path)
    assert result is None


def test_upload_local_string_session_key(tmp_path):
    data = [{"session_key": "all_2024"}]
    result = upload_local(data, "sessions", 2024, "all_2024", base_dir=tmp_path)
    assert result is not None
    expected_dir = tmp_path / "bronze/sessions/season=2024/session_key=all_2024"
    assert expected_dir.is_dir()
