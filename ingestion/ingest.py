"""
ingestion/ingest.py
────────────────────
Phase 1 orchestrator: pulls every configured endpoint for every session
in the chosen seasons and lands raw JSON in the Bronze layer.

Run locally (dry-run, no GCS):
    LOCAL_MODE=1 python -m ingestion.ingest

Run against GCS:
    python -m ingestion.ingest

Override config path:
    python -m ingestion.ingest --config /path/to/config.yaml

Flags:
    --config      Path to config.yaml (default: repo root)
    --season      Override season(s), comma-separated  (e.g. 2023,2024)
    --session-key Ingest a single session key only (useful for testing)
    --overwrite   Re-fetch and overwrite already-ingested files
    --local       Force local mode (writes to ./local_bronze/)
    --dry-run     Fetch from API but do not write anywhere (count-check only)
    --log-level   DEBUG | INFO | WARNING (default: INFO)
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Any

from dotenv import load_dotenv

from ingestion.config_loader import load_config
from ingestion.gcs_uploader import upload
from ingestion.logger import IngestionTimer, get_logger
from ingestion.openf1_client import OpenF1Client
from ingestion.rate_limiter import RateLimiter


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="F1 Bronze Ingestion")
    p.add_argument("--config", default=None, help="Path to config.yaml")
    p.add_argument("--season", default=None, help="Override seasons, comma-separated")
    p.add_argument("--session-key", default=None, type=int, help="Single session key")
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    p.add_argument("--local", action="store_true", help="Write locally instead of GCS")
    p.add_argument("--dry-run", action="store_true", help="Fetch but don't write")
    p.add_argument("--log-level", default="INFO", help="Log level")
    return p.parse_args()


def _build_client(cfg: dict, log_level: str) -> OpenF1Client:
    rl_cfg = cfg["openf1"]["rate_limit"]
    retry_cfg = cfg["openf1"]["retry"]
    rl = RateLimiter(
        rpm=rl_cfg["requests_per_minute"],
        rps=rl_cfg["requests_per_second"],
    )
    return OpenF1Client(
        base_url=cfg["openf1"]["base_url"],
        rate_limiter=rl,
        max_attempts=retry_cfg["max_attempts"],
        backoff_base=retry_cfg["backoff_base_seconds"],
        backoff_max=retry_cfg["backoff_max_seconds"],
        timeout=cfg["openf1"]["timeout_seconds"],
        logger_level=log_level,
        logger_fmt=cfg["logging"]["format"],
    )


def _get_sessions_for_season(
    client: OpenF1Client,
    cfg: dict,
    season: int,
) -> list[dict]:
    """Fetch sessions and apply max_sessions_per_season cap."""
    session_type = cfg.get("session_type_filter")   # e.g. "Race" or None
    sessions = client.get_sessions(year=season, session_type=session_type)

    max_s = cfg.get("max_sessions_per_season")
    if max_s is not None:
        sessions = sessions[:max_s]

    return sessions


def _ingest_session(
    client: OpenF1Client,
    cfg: dict,
    session: dict,
    season: int,
    log: Any,
    *,
    local_mode: bool,
    overwrite: bool,
    dry_run: bool,
) -> dict[str, int]:
    """
    Pull every configured endpoint for one session.

    Returns:
        Dict mapping endpoint → row_count for summary logging.
    """
    session_key = session.get("session_key")
    session_name = session.get("session_name", "?")
    country = session.get("country_name", "?")
    log.info(
        "Starting session ingestion",
        extra={
            "session_key": session_key,
            "session_name": session_name,
            "country": country,
            "season": season,
        },
    )

    endpoints: list[str] = cfg["endpoints"]
    car_data_cap: int | None = cfg.get("car_data_row_cap")
    bucket: str = cfg["storage"]["bucket"]

    counts: dict[str, int] = {}
    # Cache drivers list so car_data can iterate per-driver
    drivers_cache: list[dict] = []

    for endpoint in endpoints:
        # sessions and meetings are already fetched at season level
        if endpoint in ("sessions", "meetings"):
            continue

        # car_data requires driver_number; fetch per driver to avoid 422
        if endpoint == "car_data":
            data = _fetch_car_data_per_driver(
                client, session_key, drivers_cache, car_data_cap, log
            )
        else:
            params: dict[str, Any] = {"session_key": session_key}
            row_cap = None

            with IngestionTimer(log, f"fetch {endpoint} session={session_key}"):
                data = client.get(endpoint, params=params, row_cap=row_cap)

            # Cache drivers for car_data step
            if endpoint == "drivers":
                drivers_cache = data

        counts[endpoint] = len(data)
        if endpoint in ("session_result", "starting_grid") and not data:
            log.warning(
                "Beta endpoint returned empty data or was unavailable",
                extra={"endpoint": endpoint, "session_key": session_key},
            )
        else:
            log.info(
                "Endpoint fetched",
                extra={
                    "endpoint": endpoint,
                    "session_key": session_key,
                    "row_count": len(data),
                },
            )

        if dry_run:
            log.info("Dry-run: skipping upload", extra={"endpoint": endpoint})
            continue

        if not data:
            # Nothing to upload for empty array
            continue

        upload(
            data=data,
            endpoint=endpoint,
            season=season,
            session_key=session_key,
            bucket=bucket,
            local_mode=local_mode,
            overwrite=overwrite,
        )

    return counts


def _fetch_car_data_per_driver(
    client: OpenF1Client,
    session_key: int | str,
    drivers: list[dict],
    row_cap: int | None,
    log: Any,
) -> list[dict]:
    """
    Fetch car_data for each driver individually (API requires driver_number filter).
    Merges all drivers into a single list, respecting the overall row_cap.
    """
    all_data: list[dict] = []
    per_driver_cap = (row_cap // max(len(drivers), 1)) if row_cap else None

    for driver in drivers:
        driver_number = driver.get("driver_number")
        if driver_number is None:
            continue

        params: dict[str, Any] = {
            "session_key": session_key,
            "driver_number": driver_number,
        }
        with IngestionTimer(log, f"fetch car_data driver={driver_number}"):
            data = client.get("car_data", params=params, row_cap=per_driver_cap)

        all_data.extend(data)

        if row_cap and len(all_data) >= row_cap:
            all_data = all_data[:row_cap]
            log.warning(
                "car_data row cap reached across drivers",
                extra={"row_cap": row_cap, "session_key": session_key},
            )
            break

    log.info(
        "car_data fetch complete",
        extra={
            "session_key": session_key,
            "drivers_fetched": len(drivers),
            "total_rows": len(all_data),
        },
    )
    return all_data



def _ingest_sessions_endpoint(
    client: OpenF1Client,
    cfg: dict,
    sessions: list[dict],
    season: int,
    log: Any,
    *,
    local_mode: bool,
    overwrite: bool,
    dry_run: bool,
) -> None:
    """Upload the sessions list itself as a Bronze file."""
    if dry_run:
        return

    # Use a synthetic session_key = season to store the season-level file
    upload(
        data=sessions,
        endpoint="sessions",
        season=season,
        session_key=f"all_{season}",
        bucket=cfg["storage"]["bucket"],
        local_mode=local_mode,
        overwrite=overwrite,
    )
    log.info(
        "Sessions file uploaded",
        extra={"season": season, "session_count": len(sessions)},
    )


def run(args: argparse.Namespace) -> None:
    load_dotenv()   # load .env if present

    cfg = load_config(args.config)
    log = get_logger(
        "ingest",
        level=args.log_level,
        fmt=cfg["logging"]["format"],
        log_file=cfg["logging"].get("log_file"),
    )

    local_mode = args.local
    overwrite = args.overwrite
    dry_run = args.dry_run

    # Season override
    seasons = [int(s.strip()) for s in args.season.split(",")] if args.season else cfg["seasons"]

    log.info(
        "Ingestion started",
        extra={
            "seasons": seasons,
            "local_mode": local_mode,
            "dry_run": dry_run,
            "overwrite": overwrite,
        },
    )

    grand_total: dict[str, int] = {}
    run_start = time.perf_counter()

    with _build_client(cfg, args.log_level) as client:
        for season in seasons:
            log.info("Processing season", extra={"season": season})
            sessions = _get_sessions_for_season(client, cfg, season)
            log.info(
                "Sessions found",
                extra={"season": season, "count": len(sessions)},
            )

            # Upload the sessions manifest
            _ingest_sessions_endpoint(
                client, cfg, sessions, season, log,
                local_mode=local_mode, overwrite=overwrite, dry_run=dry_run,
            )

            # If user passed --session-key, filter to that session only
            if args.session_key:
                sessions = [
                    s for s in sessions
                    if s.get("session_key") == args.session_key
                ]
                if not sessions:
                    log.error(
                        "Session key not found",
                        extra={"session_key": args.session_key, "season": season},
                    )

            for session in sessions:
                counts = _ingest_session(
                    client, cfg, session, season, log,
                    local_mode=local_mode,
                    overwrite=overwrite,
                    dry_run=dry_run,
                )
                for ep, cnt in counts.items():
                    grand_total[ep] = grand_total.get(ep, 0) + cnt

    elapsed = time.perf_counter() - run_start
    log.info(
        "Ingestion complete",
        extra={
            "duration_s": round(elapsed, 2),
            "row_counts_by_endpoint": grand_total,
            "total_rows": sum(grand_total.values()),
        },
    )

    # Print a human-friendly summary table
    _print_summary(grand_total, seasons, elapsed)


def _print_summary(
    counts: dict[str, int],
    seasons: list[int],
    elapsed: float,
) -> None:
    print("\n" + "=" * 52)
    print("  F1 Bronze Ingestion - Phase 1 Summary")
    print(f"  Seasons : {', '.join(str(s) for s in seasons)}")
    print(f"  Duration: {elapsed:.1f}s")
    print("-" * 52)
    print(f"  {'Endpoint':<20} {'Total rows':>12}")
    print("-" * 52)
    for ep, cnt in sorted(counts.items()):
        print(f"  {ep:<20} {cnt:>12,}")
    print("-" * 52)
    print(f"  {'TOTAL':<20} {sum(counts.values()):>12,}")
    print("=" * 52 + "\n")


def main() -> None:
    args = _parse_args()
    try:
        run(args)
    except KeyboardInterrupt:
        print("\nInterrupted by user.", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        get_logger("ingest").exception("Fatal error", exc_info=exc)
        sys.exit(2)


if __name__ == "__main__":
    main()
