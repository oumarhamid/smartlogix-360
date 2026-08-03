from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from smartlogix.ingestion.lade import (
    LaDeDeliveryQualityValidator,
    read_lade_delivery_csv,
)


def build_valid_dataframe() -> pd.DataFrame:
    """Construit un petit dataset LaDe valide."""

    return pd.DataFrame(
        [
            {
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
            },
            {
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
            },
        ]
    )


def test_valid_dataframe_passes_blocking_contract() -> None:
    dataframe = build_valid_dataframe()

    report = LaDeDeliveryQualityValidator().validate(
        dataframe
    )

    assert report.blocking_passed is True
    assert report.blocking_failure_count == 0


def test_missing_accept_gps_is_warning() -> None:
    dataframe = build_valid_dataframe()

    report = LaDeDeliveryQualityValidator().validate(
        dataframe
    )

    warnings = {
        warning.rule: warning
        for warning in report.warnings
    }

    assert "missing_accept_gps" in warnings
    assert warnings["missing_accept_gps"].row_count == 1
    assert report.blocking_passed is True


def test_duplicate_order_id_is_blocking() -> None:
    dataframe = build_valid_dataframe()
    dataframe.loc[1, "order_id"] = 1

    report = LaDeDeliveryQualityValidator().validate(
        dataframe
    )

    assert report.blocking_passed is False
    assert report.blocking_failure_count > 0


def test_partial_accept_gps_pair_is_blocking() -> None:
    dataframe = build_valid_dataframe()
    dataframe.loc[1, "accept_gps_lng"] = 126.55
    dataframe.loc[1, "accept_gps_lat"] = None

    report = LaDeDeliveryQualityValidator().validate(
        dataframe
    )

    assert report.blocking_passed is False

    checks = {
        failure.get("check")
        for failure in report.blocking_failure_cases
    }

    assert "accept_gps_pair_consistent" in checks


def test_invalid_coordinates_are_blocking() -> None:
    dataframe = build_valid_dataframe()
    dataframe.loc[0, "lng"] = 200.0

    report = LaDeDeliveryQualityValidator().validate(
        dataframe
    )

    assert report.blocking_passed is False

    checks = {
        failure.get("check")
        for failure in report.blocking_failure_cases
    }

    assert "valid_longitude" in checks


def test_near_zero_delivery_gps_is_warning() -> None:
    dataframe = build_valid_dataframe()
    dataframe.loc[0, "delivery_gps_lng"] = 0.00004
    dataframe.loc[0, "delivery_gps_lat"] = 0.00003

    report = LaDeDeliveryQualityValidator().validate(
        dataframe
    )

    warning_rules = {
        warning.rule
        for warning in report.warnings
    }

    assert "near_zero_delivery_gps" in warning_rules
    assert report.blocking_passed is True


def test_duration_warnings_are_reported() -> None:
    dataframe = build_valid_dataframe()

    dataframe.loc[0, "delivery_time"] = (
        "09-25 08:00:00"
    )

    dataframe.loc[1, "accept_time"] = (
        "09-25 08:00:00"
    )
    dataframe.loc[1, "delivery_time"] = (
        "09-27 10:00:00"
    )

    report = LaDeDeliveryQualityValidator().validate(
        dataframe
    )

    warnings = {
        warning.rule: warning
        for warning in report.warnings
    }

    assert warnings[
        "zero_or_negative_duration"
    ].row_count == 1

    assert warnings[
        "long_delivery_duration"
    ].row_count == 1


def test_partition_mismatch_is_warning() -> None:
    dataframe = build_valid_dataframe()
    dataframe.loc[0, "ds"] = "0924"

    report = LaDeDeliveryQualityValidator().validate(
        dataframe
    )

    warning_rules = {
        warning.rule
        for warning in report.warnings
    }

    assert (
        "partition_accept_date_mismatch"
        in warning_rules
    )


def test_reader_preserves_leading_zero_in_ds(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "delivery.csv"

    build_valid_dataframe().to_csv(
        csv_path,
        index=False,
    )

    dataframe = read_lade_delivery_csv(csv_path)

    assert str(dataframe.loc[0, "ds"]) == "0925"


def test_write_quality_report(
    tmp_path: Path,
) -> None:
    dataframe = build_valid_dataframe()
    validator = LaDeDeliveryQualityValidator()

    report = validator.validate(dataframe)

    output_path = (
        tmp_path
        / "quality"
        / "delivery.quality.json"
    )

    returned_path = validator.write_report(
        report=report,
        output_path=output_path,
    )

    assert returned_path == output_path
    assert output_path.exists()

    content = json.loads(
        output_path.read_text(encoding="utf-8")
    )

    assert content["row_count"] == 2
    assert content["blocking_passed"] is True
    assert content["warning_rule_count"] >= 1