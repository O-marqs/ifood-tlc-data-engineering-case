"""Shared pytest fixtures."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from ifood_tlc.spark import get_spark_session


@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session or skip Spark tests when Java is unavailable."""
    try:
        spark = get_spark_session("ifood-tlc-pytest")
    except Exception as exc:
        pytest.skip(f"Spark is not available in this environment: {exc}")

    yield spark
    spark.stop()
