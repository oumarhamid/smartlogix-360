from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from smartlogix.ingestion.lade import (
    LaDeSilverBuilder,
    LaDeSilverBuildError,
)


def build_bronze_dataframe() -> pd.DataFrame:
    """Construit un petit DataFrame Bronze représentatif."""

    return pd.DataFrame(
        [
            {
                "order_id": 1,
                "region_id": 31,
                "city": " Jilin ",
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
                "accept_timestamp": (
                    "2000-09-25T08:00:00+00:00"
                ),
                "delivery_timestamp": (
                    "2000-09-25T09:30:00+00:00"
                ),
                "delivery_duration_minutes": 90.0,
                "_source_row_number": 2,
                "_source_file": "delivery_jl.csv",
                "_source_sha256": "a" * 64,
                "_dataset_revision": "abc123",
                "_ingested_at": (
                    "2026-08-03T16:00:00+00:00"
                ),
                "_quality_status": "valid",
                "_quality_contract_passed": True,
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
                "accept_time": "09-25 10:00:00",
                "accept_gps_time": "09-25 10:00:00",
                "accept_gps_lng": None,
                "accept_gps_lat": None,
                "delivery_time": "09-25 10:00:00",
                "delivery_gps_time": "09-25 10:00:00",
                "delivery_gps_lng": 0.00004,
                "delivery_gps_lat": 0.00003,
                "ds": "0925",
                "accept_timestamp": (
                    "2000-09-25T10:00:00+00:00"
                ),
                "delivery_timestamp": (
                    "2000-09-25T10:00:00+00:00"
                ),
                "delivery_duration_minutes": 0.0,
                "_source_row_number": 3,
                "_source_file": "delivery_jl.csv",
                "_source_sha256": "a" * 64,
                "_dataset_revision": "abc123",
                "_ingested_at": (
                    "2026-08-03T16:00:00+00:00"
                ),
                "_quality_status": "warning",
                "_quality_contract_passed": True,
            },
            {
                "order_id": 3,
                "region_id": 31,
                "city": "Jilin",
                "courier_id": 102,
                "lng": 126.61,
                "lat": 43.91,
                "aoi_id": 96,
                "aoi_type": 14,
                "accept_time": "09-25 08:00:00",
                "accept_gps_time": "09-25 08:00:00",
                "accept_gps_lng": 126.60,
                "accept_gps_lat": 43.90,
                "delivery_time": "09-26 10:00:00",
                "delivery_gps_time": "09-26 10:00:00",
                "delivery_gps_lng": 126.62,
                "delivery_gps_lat": 43.92,
                "ds": "0925",
                "accept_timestamp": (
                    "2000-09-25T08:00:00+00:00"
                ),
                "delivery_timestamp": (
                    "2000-09-26T10:00:00+00:00"
                ),
                "delivery_duration_minutes": 1560.0,
                "_source_row_number": 4,
                "_source_file": "delivery_jl.csv",
                "_source_sha256": "a" * 64,
                "_dataset_revision": "abc123",
                "_ingested_at": (
                    "2026-08-03T16:00:00+00:00"
                ),
                "_quality_status": "warning",
                "_quality_contract_passed": True,
            },
        ]
    )


def test_transform_preserves_rows_and_normalizes_data() -> None:
    dataframe = build_bronze_dataframe()

    transformed = LaDeSilverBuilder().transform(
        dataframe,
        processed_at=datetime(
            2026,
            8,
            3,
            17,
            30,
            tzinfo=UTC,
        ),
    )

    assert len(transformed) == 3
    assert transformed["city"].tolist() == [
        "Jilin",
        "Jilin",
        "Jilin",
    ]

    assert transformed["accept_hour"].tolist() == [
        8,
        10,
        8,
    ]

    assert transformed["_silver_version"].nunique() == 1


def test_transform_cleans_invalid_gps_coordinates() -> None:
    dataframe = build_bronze_dataframe()

    transformed = LaDeSilverBuilder().transform(
        dataframe
    )

    assert bool(
        transformed.loc[0, "accept_gps_valid"]
    )

    assert not bool(
        transformed.loc[1, "accept_gps_valid"]
    )

    assert not bool(
        transformed.loc[1, "delivery_gps_valid"]
    )

    assert pd.isna(
        transformed.loc[
            1,
            "delivery_gps_lng_clean",
        ]
    )

    assert transformed.loc[
        1,
        "gps_quality_status",
    ] == "missing_or_invalid_both"


def test_transform_classifies_delivery_durations() -> None:
    dataframe = build_bronze_dataframe()

    transformed = LaDeSilverBuilder().transform(
        dataframe
    )

    assert transformed[
        "delivery_duration_status"
    ].tolist() == [
        "normal",
        "zero_or_negative",
        "long",
    ]

    assert transformed[
        "is_long_delivery"
    ].tolist() == [
        False,
        False,
        True,
    ]


def test_missing_required_column_is_rejected() -> None:
    dataframe = build_bronze_dataframe().drop(
        columns=["delivery_timestamp"]
    )

    with pytest.raises(
        LaDeSilverBuildError,
        match="obligatoires",
    ):
        LaDeSilverBuilder().transform(dataframe)


def test_duplicate_order_id_is_rejected() -> None:
    dataframe = build_bronze_dataframe()
    dataframe.loc[1, "order_id"] = 1

    with pytest.raises(
        LaDeSilverBuildError,
        match="doublons",
    ):
        LaDeSilverBuilder().transform(dataframe)


def test_write_and_build_silver_parquet(
    tmp_path: Path,
) -> None:
    dataframe = build_bronze_dataframe()

    output_path = (
        tmp_path
        / "silver"
        / "delivery_jl.parquet"
    )

    result = LaDeSilverBuilder().build(
        dataframe=dataframe,
        source_path=(
            tmp_path
            / "bronze"
            / "delivery_jl.parquet"
        ),
        output_path=output_path,
    )

    assert result.row_count == 3
    assert result.quality_warning_row_count == 2
    assert result.gps_issue_row_count == 1
    assert result.duration_issue_row_count == 2
    assert result.parquet_size_bytes > 0
    assert output_path.exists()

    restored = pd.read_parquet(
        output_path,
        engine="pyarrow",
    )

    assert len(restored) == 3
    assert "gps_quality_status" in restored.columns
    assert "delivery_duration_status" in (
        restored.columns
    )