"""
ingestion/openf1_client.py
──────────────────────────
Typed OpenF1 API client with:
  • dual-window rate limiting (30 rpm / 3 rps)
  • exponential back-off with jitter on 429 / 5xx
  • optional per-endpoint row cap (car_data can be enormous)
  • structured logging of every request outcome

Docs: https://openf1.org/
"""

from __future__ import annotations

import random
import time
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ingestion.logger import IngestionTimer, get_logger
from ingestion.rate_limiter import RateLimiter

# Endpoints that accept a session_key filter directly
_SESSION_KEYED_ENDPOINTS = {
    "laps", "stints", "pit", "weather", "car_data",
    "drivers",          # also session-scoped in practice
    "position",
    "intervals",
}

# Endpoints fetched at the season/meeting level (no session_key in URL)
_SEASON_LEVEL_ENDPOINTS = {"sessions", "meetings"}


class OpenF1Client:
    """
    HTTP client for the OpenF1 REST API.

    Args:
        base_url:         API base URL (from config).
        rate_limiter:     Pre-configured RateLimiter instance.
        max_attempts:     Total attempts before giving up (default 5).
        backoff_base:     Seconds for base of exponential back-off.
        backoff_max:      Maximum seconds to wait between retries.
        timeout:          Per-request timeout in seconds.
        logger_level:     Log level for this client.
        logger_fmt:       "json" | "text".
    """

    def __init__(
        self,
        base_url: str,
        rate_limiter: RateLimiter,
        max_attempts: int = 5,
        backoff_base: float = 2.0,
        backoff_max: float = 60.0,
        timeout: int = 30,
        logger_level: str = "INFO",
        logger_fmt: str = "json",
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._rl = rate_limiter
        self._max_attempts = max_attempts
        self._backoff_base = backoff_base
        self._backoff_max = backoff_max
        self._timeout = timeout
        self._log = get_logger(__name__, level=logger_level, fmt=logger_fmt)

        # Mount an HTTPAdapter with connection-level retries (network errors only;
        # HTTP-level retries are handled manually so we can log them).
        adapter = HTTPAdapter(
            max_retries=Retry(
                total=0,          # manual retry loop handles this
                raise_on_status=False,
            )
        )
        self._session = requests.Session()
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)
        self._session.headers.update({"Accept": "application/json"})

    # ── Public API ────────────────────────────────────────────────────────

    def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        row_cap: int | None = None,
    ) -> list[dict]:
        """
        Fetch all records from *endpoint* matching *params*.

        Handles pagination automatically (OpenF1 returns up to 1 000 rows
        per request; we re-fetch with an offset until the page is empty).

        Args:
            endpoint:  e.g. "laps", "stints", "car_data".
            params:    Query parameters dict (session_key, driver_number, …).
            row_cap:   If set, stop fetching after this many rows
                       (useful for car_data in dev mode).

        Returns:
            List of record dicts (raw JSON).
        """
        url = f"{self._base_url}/{endpoint}"
        all_rows: list[dict] = []
        page_params = dict(params or {})

        while True:
            page = self._get_with_retry(url, page_params)
            if not page:
                break
            all_rows.extend(page)

            # Enforce row cap
            if row_cap is not None and len(all_rows) >= row_cap:
                all_rows = all_rows[:row_cap]
                self._log.warning(
                    "Row cap reached",
                    extra={"endpoint": endpoint, "row_cap": row_cap},
                )
                break

            # OpenF1 doesn't have an explicit next-page token; if we got a
            # full page (1 000 rows) we try to get the next slice.
            # We use the best available timestamp field as a cursor.
            if len(page) < 1000:
                break

            last = page[-1]
            # Try common timestamp fields as pagination cursor (order matters)
            cursor_field, cursor_val = None, None
            for field in ("date_utc", "date", "date_start", "timestamp"):
                if last.get(field):
                    cursor_field, cursor_val = field, last[field]
                    break

            if not cursor_val:
                # No cursor available; stop to avoid infinite loops
                self._log.warning(
                    "Cannot paginate: no timestamp field in last record",
                    extra={"endpoint": endpoint, "last_keys": list(last.keys())},
                )
                break

            # Advance cursor past the last record to avoid re-fetching boundary
            page_params = {**page_params, f"{cursor_field}>": cursor_val}

        self._log.info(
            "Fetched endpoint",
            extra={
                "endpoint": endpoint,
                "params": params,
                "row_count": len(all_rows),
            },
        )
        return all_rows

    def get_sessions(self, year: int, session_type: str | None = None) -> list[dict]:
        """
        Convenience method: fetch all sessions for a season year.

        Args:
            year:          Season year, e.g. 2024.
            session_type:  Optional filter, e.g. "Race", "Qualifying".

        Returns:
            List of session dicts.
        """
        params: dict[str, Any] = {"year": year}
        if session_type:
            params["session_type"] = session_type

        with IngestionTimer(self._log, f"get_sessions(year={year})"):
            return self.get("sessions", params=params)

    # ── Internal helpers ──────────────────────────────────────────────────

    def _get_with_retry(
        self, url: str, params: dict[str, Any]
    ) -> list[dict]:
        """
        Single-page GET with rate-limiting and exponential back-off.

        Retried on:
          • requests.exceptions.ConnectionError / Timeout
          • HTTP 429 (Too Many Requests)
          • HTTP 5xx (server errors)

        Not retried on:
          • HTTP 4xx (except 429) – indicates bad request, fail fast.
        """
        for attempt in range(1, self._max_attempts + 1):
            try:
                self._rl.acquire()                # honour rate limits FIRST
                resp = self._session.get(
                    url, params=params, timeout=self._timeout
                )
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as exc:
                self._log.warning(
                    "Network error",
                    extra={"url": url, "attempt": attempt, "error": str(exc)},
                )
                self._backoff(attempt, exc)
                continue

            if resp.status_code == 200:
                try:
                    data = resp.json()
                except ValueError as exc:
                    raise ValueError(
                        f"Non-JSON response from {url}: {resp.text[:200]}"
                    ) from exc
                return data if isinstance(data, list) else [data]

            if resp.status_code == 429 or resp.status_code >= 500:
                retry_after = float(resp.headers.get("Retry-After", 0))
                wait = max(
                    retry_after,
                    self._backoff_base ** attempt + random.uniform(0, 1),
                )
                self._log.warning(
                    "Retryable HTTP error",
                    extra={
                        "url": url,
                        "status": resp.status_code,
                        "attempt": attempt,
                        "wait_s": round(wait, 2),
                    },
                )
                time.sleep(min(wait, self._backoff_max))
                continue

            if resp.status_code == 422:
                # 422 = query too broad (too many rows). Caller must narrow params.
                self._log.warning(
                    "422 Unprocessable Entity – query too broad, returning empty",
                    extra={"url": url, "params": params},
                )
                return []

            if resp.status_code == 404:
                try:
                    err_json = resp.json()
                except Exception:
                    err_json = None

                if isinstance(err_json, dict) and err_json.get("detail") == "No results found.":
                    self._log.warning(
                        "404 No results found – returning empty",
                        extra={"url": url, "params": params},
                    )
                    return []

                # Other 404: raise error with URL and response text
                raise requests.HTTPError(
                    f"HTTP 404 Not Found for {url} with params {params}: {resp.text}",
                    response=resp,
                )

            # Other 4xx: do not retry
            resp.raise_for_status()

        raise RuntimeError(
            f"All {self._max_attempts} attempts failed for {url} "
            f"with params {params}"
        )

    def _backoff(self, attempt: int, exc: Exception) -> None:
        wait = min(
            self._backoff_base ** attempt + random.uniform(0, 1),
            self._backoff_max,
        )
        self._log.warning(
            "Backing off",
            extra={"attempt": attempt, "wait_s": round(wait, 2), "reason": str(exc)},
        )
        time.sleep(wait)

    def close(self) -> None:
        """Release the underlying connection pool."""
        self._session.close()

    def __enter__(self) -> OpenF1Client:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
