"""
ingestion/logger.py
────────────────────
Structured logger (JSON or text) used across the pipeline.
Every ingestion run logs: endpoint, session_key, row_count, gcs_path, duration_s.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path
from typing import Any


class _JsonFormatter(logging.Formatter):
    """Emit log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        log_obj: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            log_obj["exc"] = self.formatException(record.exc_info)
        # Merge any extra fields attached to the record
        for key, val in record.__dict__.items():
            if key not in {
                "args", "asctime", "created", "exc_info", "exc_text",
                "filename", "funcName", "id", "levelname", "levelno",
                "lineno", "module", "msecs", "message", "msg", "name",
                "pathname", "process", "processName", "relativeCreated",
                "stack_info", "thread", "threadName",
            }:
                log_obj[key] = val
        return json.dumps(log_obj, default=str)


def get_logger(
    name: str,
    level: str = "INFO",
    fmt: str = "json",
    log_file: str | None = None,
) -> logging.Logger:
    """
    Build and return a named logger.

    Args:
        name:     Logger name (usually __name__).
        level:    Log level string, e.g. "INFO", "DEBUG".
        fmt:      "json" or "text".
        log_file: Optional path; if given, also writes to a file.

    Returns:
        Configured Logger instance.
    """
    logger = logging.getLogger(name)
    if logger.handlers:          # avoid duplicate handlers in notebooks / tests
        return logger

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    if fmt == "json":
        formatter: logging.Formatter = _JsonFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s – %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Optional file handler
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


class IngestionTimer:
    """Context manager that measures wall-clock time and logs on exit."""

    def __init__(self, logger: logging.Logger, label: str) -> None:
        self._logger = logger
        self._label = label
        self._start: float = 0.0

    def __enter__(self) -> IngestionTimer:
        self._start = time.perf_counter()
        return self

    def __exit__(self, *_: Any) -> None:
        elapsed = time.perf_counter() - self._start
        self._logger.info(
            f"{self._label} completed",
            extra={"duration_s": round(elapsed, 3)},
        )
