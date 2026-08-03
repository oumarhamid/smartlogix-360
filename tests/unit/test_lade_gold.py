from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest

from smartlogix.ingestion.lade import (
    LaDeGoldBuilder,
    LaDeGoldBuildError,
)


def build_silver_dataframe() -> pd.DataFrame:
    """Construit un DataFrame Silver représentatif."""

    common_metadata = {
        "_source_file": "delivery_jl.csv",
        "_source_sha256": "a" * 64,
        "_dataset_revision": "abc123",
        "_silver_processed_at": (
            "2026-08-03T16:58:51+00:00"
        ),
        "_silver_version": "1.0",
    }

    return pd.DataFrame(
        [
            {
                "order_id": 1,
                "region_id": 31,
                "city": "Jilin",
                "courier_id": 100,
                "aoi_id": 10,
                "aoi_type": 14,
                "partition_timestamp": (
                    "2000-09-25T00:00:00+00:00"
                ),
                "accept_timestamp": (
                    "2000-09-25T08:00:00+00:00"
                ),
                "delivery_timestamp": (
                    "2000-09-25T09:30:00+00:00"
                ),
                "delivery_duration_minutes": 90.0,
                "delivery_duration_status": "normal",
                "accept_gps_lng_clean": 126.55,
                "accept_gps_lat_clean": 43.83,
                "delivery_gps_lng_clean": 126.57,
                "delivery_gps_lat_clean": 43.85,
                "accept_gps_valid": True,
                "delivery_gps_valid": True,
                "gps_quality_status": "complete",
                "is_quality_warning": False,
                "_quality_warning_count": 0,
                **common_metadata,
            },
            {
                "order_id": 2,
                "region_id": 31,
                "city": "Jilin",
                "courier_id": 100,
                "aoi_id": 11,
                "aoi_type": 14,
                "partition_timestamp": (
                    "2000-09-25T00:00:00+00:00"
                ),
                "accept_timestamp": (
                    "2000-09-25T10:00:00+00:00"
                ),
                "delivery_timestamp": (
                    "2000-09-25T15:00:00+00:00"
                ),
                "delivery_duration_minutes": 300.0,
                "delivery_duration_status": "normal",
                "accept_gps_lng_clean": 126.58,
                "accept_gps_lat_clean": 43.86,
                "delivery_gps_lng_clean": 126.59,
                "delivery_gps_lat_clean": 43.87,
                "accept_gps_valid": True,
                "delivery_gps_valid": True,
                "gps_quality_status": "complete",
                "is_quality_warning": True,
                "_quality_warning_count": 1,
                **common_metadata,
            },
            {
                "order_id": 3,
                "region_id": 31,
                "city": "Jilin",
                "courier_id": 101,
                "aoi_id": 12,
                "aoi_type": 14,
                "partition_timestamp": (
                    "2000-09-25T00:00:00+00:00"
                ),
                "accept_timestamp": (
                    "2000-09-25T11:00:00+00:00"
                ),
                "delivery_timestamp": (
                    "2000-09-25T11:00:00+00:00"
                ),
                "delivery_duration_minutes": 0.0,
                "delivery_duration_status": (
                    "zero_or_negative"
                ),
                "accept_gps_lng_clean": None,
                "accept_gps_lat_clean": None,
                "delivery_gps_lng_clean": None,
                "delivery_gps_lat_clean": None,
                "accept_gps_valid": False,
                "delivery_gps_valid": False,
                "gps_quality_status": (
                    "missing_or_invalid_both"
                ),
                "is_quality_warning": True,
                "_quality_warning_count": 2,
                **common_metadata,
            },
            {
                "order_id": 4,
                "region_id": 32,
                "city": "Changchun",
                "courier_id": 200,
                "aoi_id": 20,
                "aoi_type": 15,
                "partition_timestamp": (
                    "2000-09-26T00:00:00+00:00"
                ),
                "accept_timestamp": (
                    "2000-09-26T08:00:00+00:00"
                ),
                "delivery_timestamp": (
                    "2000-09-26T10:00:00+00:00"
                ),
                "delivery_duration_minutes": 120.0,
                "delivery_duration_status": "normal",
                "accept_gps_lng_clean": 125.31,
                "accept_gps_lat_clean": 43.88,
                "delivery_gps_lng_clean": None,
                "delivery_gps_lat_clean": None,
                "accept_gps_valid": True,
                "delivery_gps_valid": False,
                "gps_quality_status": (
                    "missing_or_invalid_delivery"
                ),
                "is_quality_warning": False,
                "_quality_warning_count": 0,
                **common_metadata,
            },
        ]
    )


def test_delivery_fact_preserves_and_enriches_rows() -> None:
    dataframe = build_silver_dataframe()

    tables = LaDeGoldBuilder(
        sla_minutes=240
    ).transform(
        dataframe,
        processed_at=datetime(
            2026,
            8,
            3,
            18,
            0,
            tzinfo=UTC,
        ),
    )

    fact = tables.delivery_fact

    assert len(fact) == 4
    assert fact["delivery_count"].sum() == 4
    assert fact["delivery_year"].tolist() == [
        2000,
        2000,
        2000,
        2000,
    ]
    assert fact["_gold_version"].nunique() == 1
    assert fact["_gold_version"].iloc[0] == "1.0"


def test_delivery_fact_classifies_sla() -> None:
    dataframe = build_silver_dataframe()

    fact = LaDeGoldBuilder(
        sla_minutes=240
    ).transform(
        dataframe
    ).delivery_fact

    assert fact["is_valid_duration"].tolist() == [
        True,
        True,
        False,
        True,
    ]

    assert fact["is_within_sla"].tolist() == [
        True,
        False,
        False,
        True,
    ]

    assert fact["is_late_delivery"].tolist() == [
        False,
        True,
        False,
        False,
    ]


def test_courier_daily_performance_metrics() -> None:
    dataframe = build_silver_dataframe()

    courier_daily = LaDeGoldBuilder(
        sla_minutes=240
    ).transform(
        dataframe
    ).courier_daily_performance

    courier_100 = courier_daily.loc[
        courier_daily["courier_id"].eq(100)
    ].iloc[0]

    assert courier_100["orders_total"] == 2
    assert courier_100["orders_valid_duration"] == 2
    assert courier_100["orders_within_sla"] == 1
    assert courier_100["orders_late"] == 1
    assert courier_100["unique_aois"] == 2
    assert courier_100["avg_duration_minutes"] == 195
    assert courier_100["sla_compliance_rate"] == 50
    assert courier_100["gps_completeness_rate"] == 100
    assert courier_100["quality_warning_rate"] == 50


def test_city_daily_performance_metrics() -> None:
    dataframe = build_silver_dataframe()

    city_daily = LaDeGoldBuilder(
        sla_minutes=240
    ).transform(
        dataframe
    ).city_daily_performance

    jilin = city_daily.loc[
        city_daily["city"].eq("Jilin")
    ].iloc[0]

    assert jilin["orders_total"] == 3
    assert jilin["orders_valid_duration"] == 2
    assert jilin["orders_within_sla"] == 1
    assert jilin["orders_late"] == 1
    assert jilin["unique_couriers"] == 2
    assert jilin["unique_aois"] == 3
    assert jilin["avg_duration_minutes"] == 195
    assert jilin["sla_compliance_rate"] == 50
    assert jilin["gps_completeness_rate"] == pytest.approx(
        66.67
    )
    assert jilin["quality_warning_rate"] == pytest.approx(
        66.67
    )


def test_invalid_silver_input_is_rejected() -> None:
    missing_column_dataframe = (
        build_silver_dataframe().drop(
            columns=["gps_quality_status"]
        )
    )

    with pytest.raises(
        LaDeGoldBuildError,
        match="obligatoires",
    ):
        LaDeGoldBuilder().transform(
            missing_column_dataframe
        )

    duplicate_dataframe = (
        build_silver_dataframe()
    )

    duplicate_dataframe.loc[
        1,
        "order_id",
    ] = 1

    with pytest.raises(
        LaDeGoldBuildError,
        match="doublons",
    ):
        LaDeGoldBuilder().transform(
            duplicate_dataframe
        )


def test_build_writes_three_gold_tables(
    tmp_path: Path,
) -> None:
    dataframe = build_silver_dataframe()

    output_directory = (
        tmp_path
        / "gold"
        / "lade"
        / "delivery"
    )

    result = LaDeGoldBuilder(
        sla_minutes=240
    ).build(
        dataframe=dataframe,
        source_path=(
            tmp_path
            / "silver"
            / "delivery_jl.parquet"
        ),
        output_directory=output_directory,
    )

    assert result.delivery_fact.row_count == 4
    assert (
        result.courier_daily_performance.row_count
        == 3
    )
    assert (
        result.city_daily_performance.row_count
        == 2
    )

    delivery_fact_path = (
        output_directory
        / "delivery_fact.parquet"
    )

    courier_daily_path = (
        output_directory
        / "courier_daily_performance.parquet"
    )

    city_daily_path = (
        output_directory
        / "city_daily_performance.parquet"
    )

    assert delivery_fact_path.exists()
    assert courier_daily_path.exists()
    assert city_daily_path.exists()

    restored_fact = pd.read_parquet(
        delivery_fact_path,
        engine="pyarrow",
    )

    restored_courier_daily = pd.read_parquet(
        courier_daily_path,
        engine="pyarrow",
    )

    restored_city_daily = pd.read_parquet(
        city_daily_path,
        engine="pyarrow",
    )

    assert len(restored_fact) == 4
    assert len(restored_courier_daily) == 3
    assert len(restored_city_daily) == 2