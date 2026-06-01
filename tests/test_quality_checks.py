"""Spark tests for yellow taxi quality report rules."""

from __future__ import annotations

from datetime import date, datetime

from pyspark.sql import Row

from ifood_tlc.quality.yellow_checks import QUALITY_RULE_NAMES, build_quality_report


def test_quality_report_counts_failed_records_by_rule(spark_session) -> None:
    rows = [
        Row(
            reference_year=2023,
            reference_month="05",
            tpep_pickup_datetime=datetime(2023, 5, 1, 10, 0, 0),
            tpep_dropoff_datetime=datetime(2023, 5, 1, 10, 20, 0),
            pickup_date=date(2023, 5, 1),
            total_amount=20.0,
            passenger_count=2.0,
            trip_distance=3.0,
            trip_duration_minutes=20.0,
        ),
        Row(
            reference_year=2023,
            reference_month="05",
            tpep_pickup_datetime=datetime(2023, 4, 30, 23, 50, 0),
            tpep_dropoff_datetime=datetime(2023, 4, 30, 23, 40, 0),
            pickup_date=date(2023, 4, 30),
            total_amount=-1.0,
            passenger_count=0.0,
            trip_distance=-2.0,
            trip_duration_minutes=-10.0,
        ),
    ]
    df = spark_session.createDataFrame(rows)

    report = build_quality_report(df, run_id="test-run")
    failures = {
        row["rule_name"]: row["failed_records"]
        for row in report.select("rule_name", "failed_records").collect()
    }

    assert set(failures) == set(QUALITY_RULE_NAMES)
    assert failures["dropoff_before_pickup"] == 1
    assert failures["negative_total_amount"] == 1
    assert failures["zero_passenger_count"] == 1
    assert failures["negative_trip_distance"] == 1
    assert failures["invalid_trip_duration"] == 1
    assert failures["records_outside_reference_month"] == 1
    assert failures["null_pickup_datetime"] == 0
