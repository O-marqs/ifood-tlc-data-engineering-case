"""Configuration helpers for local and Databricks execution."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "pipeline.yml"
DEFAULT_DATA_DIR = Path("data")
DATA_DIR_ENV_VAR = "DATA_DIR"


def load_pipeline_config(config_path: str | Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    """Load the YAML pipeline configuration."""
    path = Path(config_path)
    with path.open("r", encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file) or {}

    if not isinstance(config, dict):
        raise ValueError(f"Invalid pipeline config format: {path}")

    return config


def get_data_dir(data_dir: str | Path | None = None) -> Path:
    """Return the data root, using DATA_DIR when provided."""
    if data_dir is not None:
        return Path(data_dir)

    return Path(os.getenv(DATA_DIR_ENV_VAR, DEFAULT_DATA_DIR))


def resolve_data_path(configured_path: str | Path, data_dir: str | Path | None = None) -> Path:
    """Resolve a configured data path against the active data root."""
    path = Path(configured_path)
    if path.is_absolute():
        return path

    active_data_dir = get_data_dir(data_dir)
    parts = path.parts
    if parts and parts[0] == DEFAULT_DATA_DIR.name:
        return active_data_dir.joinpath(*parts[1:])

    return active_data_dir / path


def get_configured_paths(
    config: dict[str, Any] | None = None,
    data_dir: str | Path | None = None,
) -> dict[str, Path]:
    """Return landing, bronze, silver and gold paths."""
    pipeline_config = config or load_pipeline_config()
    configured_paths = pipeline_config.get("paths", {})

    return {
        layer: resolve_data_path(path, data_dir)
        for layer, path in configured_paths.items()
    }
