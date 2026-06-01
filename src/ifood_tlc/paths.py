"""Path helpers for the NYC TLC case."""

from __future__ import annotations

from pathlib import Path

from .config import get_configured_paths, load_pipeline_config


SUPPORTED_SERVICE_TYPES = ("yellow",)
DATA_LAYERS = ("landing", "bronze", "silver", "gold")


def format_month(month: int | str) -> str:
    """Format a month value with two digits."""
    month_int = int(month)
    if month_int < 1 or month_int > 12:
        raise ValueError(f"Invalid month: {month}")

    return f"{month_int:02d}"


def validate_service_type(service_type: str) -> str:
    """Validate the service type supported by this case version."""
    normalized_service_type = service_type.lower()
    if normalized_service_type not in SUPPORTED_SERVICE_TYPES:
        supported = ", ".join(SUPPORTED_SERVICE_TYPES)
        raise ValueError(
            f"Unsupported service_type '{service_type}'. "
            f"This version supports only: {supported}."
        )

    return normalized_service_type


def get_layer_path(
    layer: str,
    data_dir: str | Path | None = None,
    create: bool = False,
) -> Path:
    """Return the configured path for a data layer."""
    if layer not in DATA_LAYERS:
        raise ValueError(f"Invalid data layer: {layer}")

    layer_path = get_configured_paths(data_dir=data_dir)[layer]
    if create:
        layer_path.mkdir(parents=True, exist_ok=True)

    return layer_path


def get_landing_path(data_dir: str | Path | None = None, create: bool = False) -> Path:
    """Return the landing layer path."""
    return get_layer_path("landing", data_dir=data_dir, create=create)


def get_bronze_path(data_dir: str | Path | None = None, create: bool = False) -> Path:
    """Return the bronze layer path."""
    return get_layer_path("bronze", data_dir=data_dir, create=create)


def get_silver_path(data_dir: str | Path | None = None, create: bool = False) -> Path:
    """Return the silver layer path."""
    return get_layer_path("silver", data_dir=data_dir, create=create)


def get_gold_path(data_dir: str | Path | None = None, create: bool = False) -> Path:
    """Return the gold layer path."""
    return get_layer_path("gold", data_dir=data_dir, create=create)


def ensure_data_dirs(data_dir: str | Path | None = None) -> dict[str, Path]:
    """Create local data layer directories when they do not exist."""
    return {layer: get_layer_path(layer, data_dir=data_dir, create=True) for layer in DATA_LAYERS}


def build_landing_file_path(
    year: int | str | None = None,
    month: int | str = 1,
    service_type: str = "yellow",
    data_dir: str | Path | None = None,
) -> Path:
    """Build the expected raw parquet path in the landing zone."""
    pipeline_config = load_pipeline_config()
    resolved_service_type = validate_service_type(service_type)
    resolved_year = str(year or pipeline_config["default_year"])
    resolved_month = format_month(month)

    return (
        get_landing_path(data_dir=data_dir)
        / "tlc"
        / resolved_service_type
        / f"year={resolved_year}"
        / f"month={resolved_month}"
        / f"{resolved_service_type}_tripdata_{resolved_year}-{resolved_month}.parquet"
    )
