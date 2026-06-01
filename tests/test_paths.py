"""Unit tests for project path helpers."""

from __future__ import annotations

from ifood_tlc.paths import build_landing_file_path, format_month


def test_format_month_pads_single_digit_month() -> None:
    assert format_month(1) == "01"
    assert format_month("5") == "05"
    assert format_month(12) == "12"


def test_build_landing_file_path_uses_expected_layout(tmp_path) -> None:
    path = build_landing_file_path(
        year=2023,
        month=5,
        service_type="yellow",
        data_dir=tmp_path,
    )

    expected = (
        tmp_path
        / "landing"
        / "tlc"
        / "yellow"
        / "year=2023"
        / "month=05"
        / "yellow_tripdata_2023-05.parquet"
    )
    assert path == expected
