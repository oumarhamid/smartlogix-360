from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from smartlogix.ingestion.lade import (
    BRONZE_WARNING_COLUMNS,
    LaDeBronzeBuilder,
    LaDeBronzeBuildError,
    LaDeDeliveryQualityValidator,
)


def build_delivery_dataframe() -> pd.DataFrame:
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


def test_transform_preserves_rows_and_adds_metadata(
    tmp_path: Path,
) -> None:
    dataframe = build_delivery_dataframe()

    quality_report = (
        LaDeDeliveryQualityValidator()
        .validate(dataframe)
    )

    ingested_at = datetime(
        2026,
        8,
        3,
        16,
        30,
        tzinfo=UTC,
    )

    transformed = LaDeBronzeBuilder().transform(
        dataframe=dataframe,
        quality_report=quality_report,
        source_path=tmp_path / "delivery_jl.csv",
        source_sha256="a" * 64,
        dataset_revision="abc123",
        ingested_at=ingested_at,
    )

    assert len(transformed) == 2
    assert transformed["order_id"].tolist() == [1, 2]

    assert transformed[
        "_source_row_number"
    ].tolist() == [2, 3]

    assert transformed[
        "_source_sha256"
    ].nunique() == 1

    assert transformed[
        "_dataset_revision"
    ].tolist() == ["abc123", "abc123"]

    assert transformed[
        "_quality_status"
    ].tolist() == ["valid", "warning"]

    assert transformed[
        "_warning_missing_accept_gps"
    ].tolist() == [False, True]

    assert transformed[
        "delivery_duration_minutes"
    ].tolist() == [90.0, 45.0]

    assert str(
        transformed.loc[
            0,
            "accept_timestamp",
        ].year
    ) == "2000"


def test_transform_adds_all_warning_columns(
    tmp_path: Path,
) -> None:
    dataframe = build_delivery_dataframe()

    quality_report = (
        LaDeDeliveryQualityValidator()
        .validate(dataframe)
    )

    transformed = LaDeBronzeBuilder().transform(
        dataframe=dataframe,
        quality_report=quality_report,
        source_path=tmp_path / "delivery_jl.csv",
        source_sha256="b" * 64,
        dataset_revision="abc123",
    )

    for warning_column in (
        BRONZE_WARNING_COLUMNS.values()
    ):
        assert warning_column in transformed.columns


def test_blocking_report_prevents_bronze_build(
    tmp_path: Path,
) -> None:
    dataframe = build_delivery_dataframe()
    dataframe.loc[1, "order_id"] = 1

    quality_report = (
        LaDeDeliveryQualityValidator()
        .validate(dataframe)
    )

    with pytest.raises(
        LaDeBronzeBuildError,
        match="erreurs bloquantes",
    ):
        LaDeBronzeBuilder().transform(
            dataframe=dataframe,
            quality_report=quality_report,
            source_path=(
                tmp_path / "delivery_jl.csv"
            ),
            source_sha256="c" * 64,
            dataset_revision="abc123",
        )


def test_invalid_sha256_is_rejected(
    tmp_path: Path,
) -> None:
    dataframe = build_delivery_dataframe()

    quality_report = (
        LaDeDeliveryQualityValidator()
        .validate(dataframe)
    )

    with pytest.raises(
        LaDeBronzeBuildError,
        match="64 caractères",
    ):
        LaDeBronzeBuilder().transform(
            dataframe=dataframe,
            quality_report=quality_report,
            source_path=(
                tmp_path / "delivery_jl.csv"
            ),
            source_sha256="invalid",
            dataset_revision="abc123",
        )


def test_write_parquet_preserves_rows_and_columns(
    tmp_path: Path,
) -> None:
    dataframe = build_delivery_dataframe()

    quality_report = (
        LaDeDeliveryQualityValidator()
        .validate(dataframe)
    )

    builder = LaDeBronzeBuilder()

    transformed = builder.transform(
        dataframe=dataframe,
        quality_report=quality_report,
        source_path=tmp_path / "delivery_jl.csv",
        source_sha256="d" * 64,
        dataset_revision="abc123",
    )

    output_path = (
        tmp_path
        / "bronze"
        / "delivery_jl.parquet"
    )

    returned_path = builder.write_parquet(
        dataframe=transformed,
        output_path=output_path,
    )

    assert returned_path == output_path.resolve()
    assert output_path.exists()

    restored = pd.read_parquet(
        output_path,
        engine="pyarrow",
    )

    assert len(restored) == len(transformed)
    assert set(restored.columns) == set(
        transformed.columns
    )


def test_build_returns_bronze_summary(
    tmp_path: Path,
) -> None:
    dataframe = build_delivery_dataframe()

    quality_report = (
        LaDeDeliveryQualityValidator()
        .validate(dataframe)
    )

    output_path = (
        tmp_path
        / "bronze"
        / "delivery_jl.parquet"
    )

    result = LaDeBronzeBuilder().build(
        dataframe=dataframe,
        quality_report=quality_report,
        source_path=tmp_path / "delivery_jl.csv",
        output_path=output_path,
        source_sha256="e" * 64,
        dataset_revision="abc123",
    )

    assert result.row_count == 2
    assert result.warning_row_count == 1
    assert result.valid_row_count == 1
    assert result.compression == "zstd"
    assert result.parquet_size_bytes > 0
    assert Path(result.output_path).exists()