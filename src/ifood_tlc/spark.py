"""Spark session helpers for local and Databricks-compatible execution."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "ifood-tlc-case") -> SparkSession:
    """Create or reuse a Spark session."""
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    if not os.getenv("DATABRICKS_RUNTIME_VERSION"):
        builder = builder.master("local[*]")

    return builder.getOrCreate()
