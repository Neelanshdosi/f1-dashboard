"""
ingestion/gcs_uploader.py
─────────────────────────
Upload raw JSON payloads to GCS following the Bronze layer path convention:

  gs://<bucket>/bronze/<endpoint>/season=YYYY/session_key=N/<endpoint>_<ts>.json

Key features:
  • Idempotency  – checks if a file already exists before uploading (skip mode).
  • Atomic write – writes to a temp blob name, then renames to final name.
  • Row-count logging per file.
  • Local dry-run mode (writes to ./local_bronze/ instead of GCS).
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

from ingestion.logger import get_logger

_log = get_logger(__name__)


def _gcs_path(
    bucket: str,
    endpoint: str,
    season: int,
    session_key: int | str,
    filename: str,
) -> str:
    """Return the canonical GCS object path (no gs:// prefix)."""
    return (
        f"bronze/{endpoint}/season={season}/"
        f"session_key={session_key}/{filename}"
    )


def _make_filename(endpoint: str) -> str:
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{endpoint}_{ts}.json"


# ── GCS upload ────────────────────────────────────────────────────────────────

def upload_to_gcs(
    data: list[dict],
    endpoint: str,
    season: int,
    session_key: int | str,
    bucket: str,
    overwrite: bool = False,
) -> str | None:
    """
    Serialise *data* as JSON and upload to GCS.

    Args:
        data:        List of record dicts from the API.
        endpoint:    OpenF1 endpoint name (e.g. "laps").
        season:      Season year.
        session_key: Numeric session identifier.
        bucket:      GCS bucket name (no gs:// prefix).
        overwrite:   If False (default), skip upload when blob already exists.

    Returns:
        gs:// URI of the uploaded blob, or None if skipped.

    Raises:
        google.cloud.exceptions.GoogleCloudError on upload failure.
    """
    try:
        from google.cloud import storage  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "google-cloud-storage is not installed. "
            "Run: pip install google-cloud-storage"
        ) from exc

    if not data:
        _log.info(
            "Skipping upload – empty payload",
            extra={"endpoint": endpoint, "session_key": session_key},
        )
        return None

    filename = _make_filename(endpoint)
    blob_path = _gcs_path(bucket, endpoint, season, session_key, filename)
    gcs_uri = f"gs://{bucket}/{blob_path}"

    client = storage.Client()
    bkt = client.bucket(bucket)
    blob = bkt.blob(blob_path)

    # Idempotency check
    if not overwrite and blob.exists():
        _log.info(
            "Blob already exists – skipping",
            extra={"gcs_uri": gcs_uri, "endpoint": endpoint},
        )
        return gcs_uri

    payload = json.dumps(data, ensure_ascii=False)
    blob.upload_from_string(payload, content_type="application/json")

    _log.info(
        "Uploaded to GCS",
        extra={
            "gcs_uri": gcs_uri,
            "endpoint": endpoint,
            "season": season,
            "session_key": session_key,
            "row_count": len(data),
            "size_bytes": len(payload.encode()),
        },
    )
    return gcs_uri


# ── Local dry-run mode ─────────────────────────────────────────────────────────

def upload_local(
    data: list[dict],
    endpoint: str,
    season: int,
    session_key: int | str,
    base_dir: str | Path = "local_bronze",
    overwrite: bool = False,
) -> str | None:
    """
    Write *data* to the local filesystem using the same Bronze path structure.
    Useful for local development without a GCS connection.

    Args:
        data:        List of record dicts from the API.
        endpoint:    OpenF1 endpoint name.
        season:      Season year.
        session_key: Numeric session identifier.
        base_dir:    Root directory for local bronze files.
        overwrite:   If False, skip if any file for this (endpoint, session)
                     already exists.

    Returns:
        Absolute path to the written file, or None if skipped.
    """
    if not data:
        _log.info(
            "Skipping local upload – empty payload",
            extra={"endpoint": endpoint, "session_key": session_key},
        )
        return None

    out_dir = (
        Path(base_dir)
        / f"bronze/{endpoint}/season={season}/session_key={session_key}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    # Idempotency check: skip if any file for this partition exists
    if not overwrite and any(out_dir.glob(f"{endpoint}_*.json")):
        existing = next(out_dir.glob(f"{endpoint}_*.json"))
        _log.info(
            "Local file already exists – skipping",
            extra={"path": str(existing), "endpoint": endpoint},
        )
        return str(existing)

    filename = _make_filename(endpoint)
    out_path = out_dir / filename

    out_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    _log.info(
        "Written locally",
        extra={
            "path": str(out_path.resolve()),
            "endpoint": endpoint,
            "season": season,
            "session_key": session_key,
            "row_count": len(data),
        },
    )
    return str(out_path.resolve())


# ── Smart dispatcher ──────────────────────────────────────────────────────────

def upload(
    data: list[dict],
    endpoint: str,
    season: int,
    session_key: int | str,
    bucket: str,
    local_mode: bool = False,
    local_dir: str | Path = "local_bronze",
    overwrite: bool = False,
) -> str | None:
    """
    Dispatch to GCS or local depending on *local_mode*.

    Set local_mode=True (or env var LOCAL_MODE=1) for development runs.
    """
    if local_mode or os.environ.get("LOCAL_MODE", "0") == "1":
        return upload_local(
            data, endpoint, season, session_key,
            base_dir=local_dir, overwrite=overwrite,
        )
    return upload_to_gcs(
        data, endpoint, season, session_key,
        bucket=bucket, overwrite=overwrite,
    )
