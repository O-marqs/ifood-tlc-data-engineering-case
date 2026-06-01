"""Tests for idempotent all-config-months processing."""

from __future__ import annotations

from pyspark.sql import Row

from ifood_tlc.ingestion import bronze
from ifood_tlc.transformations import silver_yellow


class FakeDataFrame:
    """Minimal placeholder for monkeypatched dataframe flow."""


def test_bronze_all_config_months_forces_single_overwrite(monkeypatch) -> None:
    captured: dict[str, object] = {"writes": 0, "months_read": []}

    def fake_build_bronze_dataframe(**kwargs):
        captured["months_read"].append(kwargs["month"])
        return FakeDataFrame()

    def fake_write_bronze(df, output_path=None, write_mode="overwrite"):
        captured["writes"] += 1
        captured["write_mode"] = write_mode
        return 30

    monkeypatch.setattr(bronze, "load_pipeline_config", lambda: {"default_year": 2023, "months": [1, 2]})
    monkeypatch.setattr(bronze, "build_bronze_dataframe", fake_build_bronze_dataframe)
    monkeypatch.setattr(bronze, "union_dataframes_by_name", lambda dataframes: FakeDataFrame())
    monkeypatch.setattr(bronze, "collect_reference_month_counts", lambda df: {"01": 10, "02": 20})
    monkeypatch.setattr(bronze, "write_bronze", fake_write_bronze)

    result = bronze.ingest_config_months(
        spark=object(),
        service_type="yellow",
        year=2023,
        write_mode="append",
    )

    assert captured["months_read"] == [1, 2]
    assert captured["writes"] == 1
    assert captured["write_mode"] == "overwrite"
    assert result == {"01": 10, "02": 20}


def test_silver_all_config_months_forces_single_overwrite(monkeypatch) -> None:
    captured: dict[str, object] = {"writes": 0, "months_read": []}

    def fake_read_bronze_yellow(spark, year, month):
        captured["months_read"].append(month)
        return FakeDataFrame()

    def fake_write_silver(df, output_path=None, write_mode="overwrite"):
        captured["writes"] += 1
        captured["write_mode"] = write_mode
        return 30

    monkeypatch.setattr(silver_yellow, "load_pipeline_config", lambda: {"default_year": 2023, "months": [1, 2]})
    monkeypatch.setattr(silver_yellow, "read_bronze_yellow", fake_read_bronze_yellow)
    monkeypatch.setattr(silver_yellow, "transform_bronze_to_silver", lambda df: FakeDataFrame())
    monkeypatch.setattr(silver_yellow, "union_dataframes_by_name", lambda dataframes: FakeDataFrame())
    monkeypatch.setattr(silver_yellow, "collect_reference_month_counts", lambda df: {"01": 10, "02": 20})
    monkeypatch.setattr(silver_yellow, "write_silver", fake_write_silver)

    result = silver_yellow.transform_config_months(
        spark=object(),
        year=2023,
        write_mode="append",
    )

    assert captured["months_read"] == [1, 2]
    assert captured["writes"] == 1
    assert captured["write_mode"] == "overwrite"
    assert result == {"01": 10, "02": 20}


def test_union_dataframes_by_name_preserves_distinct_reference_months(spark_session) -> None:
    january_df = spark_session.createDataFrame([Row(reference_month="01", total_records=10)])
    february_df = spark_session.createDataFrame([Row(reference_month="02", total_records=20)])

    bronze_df = bronze.union_dataframes_by_name([january_df, february_df])
    silver_df = silver_yellow.union_dataframes_by_name([january_df, february_df])

    bronze_months = [row["reference_month"] for row in bronze_df.select("reference_month").orderBy("reference_month").collect()]
    silver_months = [row["reference_month"] for row in silver_df.select("reference_month").orderBy("reference_month").collect()]

    assert bronze_months == ["01", "02"]
    assert silver_months == ["01", "02"]
