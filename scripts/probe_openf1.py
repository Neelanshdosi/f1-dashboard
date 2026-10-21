"""Probe OpenF1 API for race sessions, qualifying sessions, and starting grid."""

import json
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

API = "https://api.openf1.org/v1"
RACE_SESSION_KEYS = [9472, 9693, 10014, 9979, 9947, 9839, 9662]


def safe_get(session, url, params=None):
    import time
    for attempt in range(5):
        time.sleep(2.1)
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code != 429:
            return resp
        print(f"  [429 Rate limited, sleeping 5s before retry {attempt+1}...]")
        time.sleep(5)
    return resp


def main():
    session = requests.Session()
    from ingestion.openf1_client import resolve_qualifying_session

    print("=== OPENF1 PROBE REPORT ===")
    results_summary = []

    for race_key in RACE_SESSION_KEYS:
        print("\n------------------------------------------------------------")
        print(f"RACE SESSION_KEY: {race_key}")

        # 1. GET /v1/sessions?session_key=N
        race_url = f"{API}/sessions"
        resp_race = safe_get(session, race_url, params={"session_key": race_key})
        print(f"GET {resp_race.url} -> HTTP {resp_race.status_code}")
        if resp_race.status_code != 200:
            print(f"ERROR: {resp_race.text}")
            continue

        race_data = resp_race.json()
        if not race_data:
            print("ERROR: Empty sessions response")
            continue

        sess_row = race_data[0]
        trimmed_sess = {
            "session_key": sess_row.get("session_key"),
            "session_name": sess_row.get("session_name"),
            "meeting_key": sess_row.get("meeting_key"),
            "year": sess_row.get("year"),
            "date_start": sess_row.get("date_start"),
            "circuit_short_name": sess_row.get("circuit_short_name"),
            "country_name": sess_row.get("country_name"),
        }
        print("Raw Session (key fields):", json.dumps(trimmed_sess, indent=2))

        meeting_key = sess_row.get("meeting_key")

        # 2. GET /v1/sessions?meeting_key=<M>&session_name=Qualifying
        qual_url = f"{API}/sessions"
        resp_qual = safe_get(session, qual_url, params={"meeting_key": meeting_key, "session_name": "Qualifying"})
        print(f"GET {resp_qual.url} -> HTTP {resp_qual.status_code}")
        qual_data = resp_qual.json() if resp_qual.status_code == 200 else []
        qual_session_keys = [q.get("session_key") for q in qual_data if q.get("session_name") == "Qualifying"]
        print("Raw Result of Qualifying session query:", json.dumps(qual_data, indent=2))
        print("Extracted Qualifying session_key list:", qual_session_keys)

        # 3. Also check all sessions for meeting_key to see what sessions actually exist
        resp_all_m = safe_get(session, qual_url, params={"meeting_key": meeting_key})
        all_m_sessions = [
            {"session_key": s.get("session_key"), "session_name": s.get("session_name"), "session_type": s.get("session_type")}
            for s in (resp_all_m.json() if resp_all_m.status_code == 200 else [])
        ]
        print(f"All sessions for meeting_key {meeting_key}:", json.dumps(all_m_sessions, indent=2))

        # 4. Starting grid query for each qualifying key found
        grid_status = "N/A"
        grid_rows_count = 0
        for qk in qual_session_keys:
            grid_url = f"{API}/starting_grid"
            resp_grid = safe_get(session, grid_url, params={"session_key": qk})
            grid_status = str(resp_grid.status_code)
            grid_data = resp_grid.json() if resp_grid.status_code == 200 else []
            grid_rows_count = len(grid_data) if isinstance(grid_data, list) else 0
            print(f"GET {resp_grid.url} -> HTTP {resp_grid.status_code}, Rows: {grid_rows_count}")
            if resp_grid.status_code != 200:
                print("Body:", resp_grid.text)

        # 5. resolve_qualifying_session return value
        resolved_qk = resolve_qualifying_session(session, race_key)
        print(f"resolve_qualifying_session(session, {race_key}) returned: {resolved_qk}")

        results_summary.append({
            "race_session_key": race_key,
            "session_name": sess_row.get("session_name"),
            "meeting_key": meeting_key,
            "year": sess_row.get("year"),
            "country_name": sess_row.get("country_name"),
            "qualifying_session_key": resolved_qk,
            "grid_http_status": grid_status,
            "grid_rows": grid_rows_count,
        })

    print("\n\n=== SUMMARY TABLE ===")
    print("| Race Key | Meeting Key | Year | Country | Qualifying Key | Grid HTTP | Grid Rows |")
    print("|---|---|---|---|---|---|---:|")
    for r in results_summary:
        print(f"| {r['race_session_key']} | {r['meeting_key']} | {r['year']} | {r['country_name']} | {r['qualifying_session_key']} | {r['grid_http_status']} | {r['grid_rows']} |")


if __name__ == "__main__":
    main()
