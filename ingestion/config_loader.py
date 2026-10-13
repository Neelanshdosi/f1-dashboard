"""
ingestion/config_loader.py
──────────────────────────
Loads config.yaml and resolves ${ENV_VAR} placeholders from the environment.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml

_ENV_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _resolve_env(value: Any) -> Any:
    """Recursively resolve ${VAR} placeholders in strings."""
    if isinstance(value, str):
        def replacer(match: re.Match) -> str:
            var = match.group(1)
            resolved = os.environ.get(var)
            if resolved is None:
                raise OSError(
                    f"Environment variable '{var}' is not set. "
                    f"Copy .env.example → .env and fill it in."
                )
            return resolved

        return _ENV_PATTERN.sub(replacer, value)
    if isinstance(value, dict):
        return {k: _resolve_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env(v) for v in value]
    return value


def load_config(config_path: str | Path | None = None) -> dict:
    """
    Load and return the resolved pipeline configuration.

    Args:
        config_path: Path to config.yaml. Defaults to <repo-root>/config.yaml.

    Returns:
        Fully resolved config dict (env-vars substituted).
    """
    if config_path is None:
        config_path = Path(__file__).resolve().parents[1] / "config.yaml"

    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open() as fh:
        raw = yaml.safe_load(fh)

    return _resolve_env(raw)
