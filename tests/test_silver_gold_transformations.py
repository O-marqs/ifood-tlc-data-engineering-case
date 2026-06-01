"""Spark tests for silver and gold transformation rules."""

from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import Row

from ifood_tlc.transformations.gold_yellow import (
    REQUIRED_DETAIL_COLUMNS,
    build_gold_yellow_trips,
)
from ifood_tlc.transformations.silver_yellow import transform_bronze_to_silver


@pytest.fixture()
def bronze_yellow_df(spark_session):
    """Create a tiny bronze-like yellow dataframe."""
    rows = [
        Row(
            VendorID=1,
            tpep_pickup_datetime=datetime(2023, 1, 1, 10, 0, 0),
            tpep_dropoff_datetime=datetime(2023, 1, 1, 10, 30, 0),
            passenger_count=2.0,
            trip_distance=3.2,
            PULocationID=100,
            DOLocationID=200,
            payment_type=1,
            fare_amount=18.0,
            tip_amount=3.0,
            tolls_amount=0.0,
            total_amount=24.5,
            reference_year=2023,
            reference_month="01",
            service_type="yellow",
            source_file="landing/yellow_2023_01.parquet",
            ingestion_run_id="run-1",
        ),
        Row(
            VendorID=2,
            tpep_pickup_datetime=datetime(2023, 1, 1, 11, 0, 0),
            tpep_dropoff_datetime=datetime(2023, 1, 1, 10, 45, 0),
            passenger_count=0.0,
            trip_distance=-1.0,
            PULocationID=101,
            DOLocationID=201,
            payment_type=2,
            fare_amount=-5.0,
            tip_amount=0.0,
            tolls_amount=0.0,
            total_amount=-5.0,
            reference_year=2023,
            reference_month="01",
            service_type="yellow",
            source_file="landing/yellow_2023_01.parquet",
            ingestion_run_id="run-1",
        ),
    ]
    return spark_session.createDataFrame(rows)


def test_silver_calculates_pickup_hour_and_trip_duration(bronze_yellow_df) -> None:
    silver_df = transform_bronze_to_silver(bronze_yellow_df)
    result = {
        row["VendorID"]: (row["pickup_hour"], row["trip_duration_minutes"])
        for row in silver_df.select("VendorID", "pickup_hour", "trip_duration_minutes").collect()
    }

    assert result[1] == (10, 30.0)
    assert result[2] == (11, -15.0)


def test_silver_does_not_remove_suspicious_records(bronze_yellow_df) -> None:
    silver_df = transform_bronze_to_silver(bronze_yellow_df)

    assert silver_df.count() == bronze_yellow_df.count()
    assert silver_df.filter("total_amount < 0").count() == 1
    assert silver_df.filter("trip_duration_minutes < 0").count() == 1


def test_gold_yellow_trips_contains_case_required_columns(bronze_yellow_df) -> None:
    silver_df = transform_bronze_to_silver(bronze_yellow_df)
    gold_df = build_gold_yellow_trips(silver_df)

    for column_name in REQUIRED_DETAIL_COLUMNS:
        assert column_name in gold_df.columns
