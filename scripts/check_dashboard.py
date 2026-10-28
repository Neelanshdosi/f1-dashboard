"""Playwright dashboard check and screenshot capture for all races.

Spins up a local HTTP server serving dashboard/, navigates to each race,
verifies that results render without console errors, extracts the top 5
rendered rows, captures desktop screenshots per race, and captures a 390px
mobile screenshot.
"""

import http.server
import json
import threading
import time
from functools import partial
from pathlib import Path

from playwright.sync_api import sync_playwright

REPO_ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = REPO_ROOT / "dashboard"
IMAGES_DIR = REPO_ROOT / "docs" / "images"
IMAGES_DIR.mkdir(parents=True, exist_ok=True)

PORT = 8088


def run_checks():
    handler = partial(http.server.SimpleHTTPRequestHandler, directory=str(DASHBOARD_DIR))
    httpd = http.server.HTTPServer(("127.0.0.1", PORT), handler)
    server_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    server_thread.start()
    print(f"HTTP server started at http://127.0.0.1:{PORT}")

    # Load gold_data.json to get race list
    with open(DASHBOARD_DIR / "gold_data.json", encoding="utf-8") as f:
        gold_data = json.load(f)

    races = gold_data.get("races", [])
    print(f"Found {len(races)} races in gold_data.json")

    results_report = []

    with sync_playwright() as p:
        browser = p.chromium.launch()

        # Desktop Context
        context = browser.new_context(viewport={"width": 1440, "height": 900})
        page = context.new_page()

        console_errors = []
        page_errors = []

        page.on("console", lambda msg: console_errors.append(f"{msg.type}: {msg.text}") if msg.type == "error" else None)
        page.on("pageerror", lambda exc: page_errors.append(str(exc)))

        for _idx, race in enumerate(races):
            session_key = race["session_key"]
            race_name = race["name"]
            console_errors.clear()
            page_errors.clear()

            url = f"http://127.0.0.1:{PORT}/#race={session_key}&tab=results"
            page.goto(url)
            page.wait_for_selector("#results-tbody tr", state="attached", timeout=10000)
            time.sleep(0.5)

            # Extract rendered rows
            rows = page.locator("#results-tbody tr").all()
            row_count = len(rows)

            extracted_rows = []
            for row in rows[:5]:
                cells = [c.inner_text().strip() for c in row.locator("td").all()]
                extracted_rows.append(cells)

            # Screenshot
            img_path = IMAGES_DIR / f"results_{session_key}.png"
            page.screenshot(path=str(img_path), full_page=False)

            results_report.append({
                "session_key": session_key,
                "name": race_name,
                "rows_rendered": row_count,
                "console_errors": len(console_errors) + len(page_errors),
                "top_5": extracted_rows,
                "screenshot": str(img_path.name),
            })

            print(f"Race {session_key} ({race_name}): {row_count} rows, {len(console_errors)} console errors -> {img_path.name}")

        context.close()

        # Mobile Context (390px viewport e.g. iPhone 12/13/14)
        mobile_context = browser.new_context(
            viewport={"width": 390, "height": 844},
            user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15",
            is_mobile=True,
            has_touch=True,
        )
        mobile_page = mobile_context.new_page()
        first_session = races[0]["session_key"] if races else 9472
        mobile_url = f"http://127.0.0.1:{PORT}/#race={first_session}&tab=results"
        mobile_page.goto(mobile_url)
        mobile_page.wait_for_selector("#results-tbody tr", state="attached", timeout=10000)
        time.sleep(0.5)

        mobile_img = IMAGES_DIR / "results_mobile_390px.png"
        mobile_page.screenshot(path=str(mobile_img), full_page=False)
        print(f"Mobile 390px screenshot saved -> {mobile_img.name}")
        mobile_context.close()

        browser.close()

    httpd.shutdown()

    # Output JSON summary
    summary_path = REPO_ROOT / "docs" / "playwright_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(results_report, f, indent=2)

    print("\n=== PLAYWRIGHT SUMMARY TABLE ===")
    print(f"{'Race':<28} | {'Session':<8} | {'Rows':<6} | {'Console Errors':<14}")
    print("-" * 62)
    for r in results_report:
        print(f"{r['name']:<28} | {r['session_key']:<8} | {r['rows_rendered']:<6} | {r['console_errors']:<14}")


if __name__ == "__main__":
    run_checks()
