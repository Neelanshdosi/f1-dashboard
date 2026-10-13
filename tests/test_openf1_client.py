"""
tests/test_openf1_client.py
────────────────────────────
Unit tests for OpenF1Client – HTTP interaction is mocked via
responses library (no real network calls).
"""

from __future__ import annotations

import pytest
import requests
import responses as resp_lib

from ingestion.openf1_client import OpenF1Client
from ingestion.rate_limiter import RateLimiter

BASE_URL = "https://api.openf1.org/v1"


def _make_client() -> OpenF1Client:
    """Return a client with a permissive rate-limiter for tests."""
    rl = RateLimiter(rpm=300, rps=100)
    return OpenF1Client(
        base_url=BASE_URL,
        rate_limiter=rl,
        max_attempts=3,
        backoff_base=0.01,   # tiny backoff so tests run fast
        backoff_max=0.05,
        timeout=5,
        logger_level="WARNING",  # suppress noise in test output
        logger_fmt="text",
    )


# ── Successful fetch ──────────────────────────────────────────────────────────

@resp_lib.activate
def test_get_returns_records():
    resp_lib.add(
        resp_lib.GET,
        f"{BASE_URL}/laps",
        json=[{"session_key": 9158, "lap_number": 1, "lap_duration": 92.5}],
        status=200,
    )
    with _make_client() as client:
        data = client.get("laps", params={"session_key": 9158})
    assert len(data) == 1
    assert data[0]["lap_number"] == 1


@resp_lib.activate
def test_get_empty_endpoint_returns_empty_list():
    resp_lib.add(resp_lib.GET, f"{BASE_URL}/pit", json=[], status=200)
    with _make_client() as client:
        data = client.get("pit", params={"session_key": 9999})
    assert data == []


# ── Retry logic ───────────────────────────────────────────────────────────────

@resp_lib.activate
def test_retries_on_500_then_succeeds():
    """First call returns 500, second returns 200 with data."""
    resp_lib.add(resp_lib.GET, f"{BASE_URL}/stints", status=500)
    resp_lib.add(
        resp_lib.GET,
        f"{BASE_URL}/stints",
        json=[{"session_key": 9158, "stint_number": 1}],
        status=200,
    )
    with _make_client() as client:
        data = client.get("stints", params={"session_key": 9158})
    assert len(data) == 1


@resp_lib.activate
def test_raises_after_max_retries():
    """All attempts return 500 – should raise RuntimeError."""
    for _ in range(3):   # matches max_attempts=3
        resp_lib.add(resp_lib.GET, f"{BASE_URL}/weather", status=500)
    with _make_client() as client, pytest.raises(RuntimeError, match="All 3 attempts failed"):
        client.get("weather", params={"session_key": 9158})


@resp_lib.activate
def test_respects_429_and_retries():
    """A 429 response should trigger a retry and eventually succeed."""
    resp_lib.add(
        resp_lib.GET, f"{BASE_URL}/laps",
        status=429,
        headers={"Retry-After": "0"},
    )
    resp_lib.add(
        resp_lib.GET, f"{BASE_URL}/laps",
        json=[{"lap_number": 5}],
        status=200,
    )
    with _make_client() as client:
        data = client.get("laps", params={"session_key": 1})
    assert data[0]["lap_number"] == 5


@resp_lib.activate
def test_no_retry_on_404():
    """HTTP 404 should not be retried – raise immediately."""
    resp_lib.add(resp_lib.GET, f"{BASE_URL}/laps", status=404)
    with _make_client() as client, pytest.raises(requests.HTTPError):
        client.get("laps", params={"session_key": 0})
    # Only 1 call should have been made
    assert len(resp_lib.calls) == 1


# ── Row cap ───────────────────────────────────────────────────────────────────

@resp_lib.activate
def test_row_cap_truncates_large_response():
    """row_cap=2 should return exactly 2 rows even if API returns more."""
    big_page = [{"date_utc": f"2024-03-{i:02d}T00:00:00", "v": i} for i in range(10)]
    resp_lib.add(resp_lib.GET, f"{BASE_URL}/car_data", json=big_page, status=200)
    with _make_client() as client:
        data = client.get("car_data", params={"session_key": 9158}, row_cap=2)
    assert len(data) == 2


# ── get_sessions helper ───────────────────────────────────────────────────────

@resp_lib.activate
def test_get_sessions_filters_by_year():
    sessions = [
        {"session_key": 1, "year": 2024, "session_type": "Race"},
        {"session_key": 2, "year": 2024, "session_type": "Qualifying"},
    ]
    resp_lib.add(resp_lib.GET, f"{BASE_URL}/sessions", json=sessions, status=200)
    with _make_client() as client:
        data = client.get_sessions(year=2024)
    assert len(data) == 2
    # Verify year param was sent
    assert resp_lib.calls[0].request.url is not None
    assert "year=2024" in resp_lib.calls[0].request.url
