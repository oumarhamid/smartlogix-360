from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from smartlogix.ingestion.lade import (
    LaDeCsvProfiler,
    LaDeCsvProfilingError,
)


def build_delivery_dataframe() -> pd.DataFrame:
    """Construit un petit dataset LaDe représentatif."""

    first_row = {
        "order_id": 1,
        "region_id": 31,
        "city": "Jilin",
        "courier_id": 100,
        "lng": 126.56,
        "lat": 43.84,
        "aoi_id": 94,
        "aoi_type": 14,
        "accept_time": "09-25 08:00:00",
        "accept_gps_time": "09-25 08:00:00",
        "accept_gps_lng": 126.55,
        "accept_gps_lat": 43.83,
        "delivery_time": "09-25 09:30:00",
        "delivery_gps_time": "09-25 09:30:00",
        "delivery_gps_lng": 126.57,
        "delivery_gps_lat": 43.85,
        "ds": "0925",
    }

    second_row = dict(first_row)

    third_row = {
        "order_id": 2,
        "region_id": 31,
        "city": "Jilin",
        "courier_id": 101,
        "lng": 126.60,
        "lat": 43.90,
        "aoi_id": 95,
        "aoi_type": 14,
        "accept_time": "09-25 23:30:00",
        "accept_gps_time": "09-25 23:30:00",
        "accept_gps_lng": None,
        "accept_gps_lat": None,
        "delivery_time": "09-25 00:15:00",
        "delivery_gps_time": "09-25 00:15:00",
        "delivery_gps_lng": 126.61,
        "delivery_gps_lat": 43.91,
        "ds": "0925",
    }

    return pd.DataFrame(
        [
            first_row,
            second_row,
            third_row,
        ]
    )


def test_profile_delivery_csv(tmp_path: Path) -> None:
    csv_path = tmp_path / "delivery_jl.csv"

    build_delivery_dataframe().to_csv(
        csv_path,
        index=False,
    )

    profile = LaDeCsvProfiler().profile(csv_path)

    assert profile.row_count == 3
    assert profile.column_count == 17
    assert profile.duplicate_row_count == 1
    assert profile.missing_columns == ()
    assert profile.unexpected_columns == ()

    assert profile.delivery_duration_minutes == {
        "valid_count": 3,
        "missing_or_invalid_count": 0,
        "minimum": 45.0,
        "maximum": 90.0,
        "mean": 75.0,
        "median": 90.0,
        "midnight_rollover_count": 1,
    }


def test_profile_detects_null_values(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "delivery_jl.csv"

    build_delivery_dataframe().to_csv(
        csv_path,
        index=False,
    )

    profile = LaDeCsvProfiler().profile(csv_path)

    columns = {
        column.name: column
        for column in profile.columns
    }

    assert columns["accept_gps_lng"].null_count == 1
    assert columns["accept_gps_lng"].non_null_count == 2
    assert columns["accept_gps_lng"].null_percentage == (
        33.333
    )


def test_profile_calculates_coordinate_bounds(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "delivery_jl.csv"

    build_delivery_dataframe().to_csv(
        csv_path,
        index=False,
    )

    profile = LaDeCsvProfiler().profile(csv_path)

    assert profile.coordinate_bounds["lng"] == {
        "minimum": 126.56,
        "maximum": 126.6,
    }

    assert profile.coordinate_bounds[
        "accept_gps_lng"
    ] == {
        "minimum": 126.55,
        "maximum": 126.55,
    }


def test_profile_reports_schema_differences(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "partial.csv"

    dataframe = pd.DataFrame(
        {
            "order_id": [1],
            "unexpected_field": ["value"],
        }
    )

    dataframe.to_csv(
        csv_path,
        index=False,
    )

    profile = LaDeCsvProfiler().profile(csv_path)

    assert "city" in profile.missing_columns
    assert profile.unexpected_columns == (
        "unexpected_field",
    )


def test_profile_rejects_missing_file(
    tmp_path: Path,
) -> None:
    missing_path = tmp_path / "missing.csv"

    with pytest.raises(
        LaDeCsvProfilingError,
        match="introuvable",
    ):
        LaDeCsvProfiler().profile(missing_path)


def test_write_profile_report(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "delivery_jl.csv"
    report_path = tmp_path / "profile.json"

    build_delivery_dataframe().to_csv(
        csv_path,
        index=False,
    )

    profiler = LaDeCsvProfiler()
    profile = profiler.profile(csv_path)

    returned_path = profiler.write_report(
        profile=profile,
        output_path=report_path,
    )

    assert returned_path == report_path
    assert report_path.exists()

    report = json.loads(
        report_path.read_text(encoding="utf-8")
    )

    assert report["row_count"] == 3
    assert report["column_count"] == 17
    assert report["duplicate_row_count"] == 1