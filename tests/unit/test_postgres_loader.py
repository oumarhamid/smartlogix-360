from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest

from smartlogix.storage import (
    PostgresGoldLoader,
    PostgresGoldLoadError,
)


def build_delivery_fact() -> pd.DataFrame:
    """Construit une petite table de faits Gold."""

    return pd.DataFrame(
        [
            {
                "order_id": 1,
                "delivery_date": (
                    "2000-09-25T00:00:00+00:00"
                ),
                "region_id": 31,
                "city": "Jilin",
                "courier_id": 100,
                "delivery_duration_minutes": 90.0,
                "is_valid_duration": True,
                "is_within_sla": True,
                "is_late_delivery": False,
                "has_complete_gps": True,
                "is_quality_warning": False,
                "delivery_count": 1,
            },
            {
                "order_id": 2,
                "delivery_date": (
                    "2000-09-25T00:00:00+00:00"
                ),
                "region_id": 31,
                "city": "Jilin",
                "courier_id": 100,
                "delivery_duration_minutes": 300.0,
                "is_valid_duration": True,
                "is_within_sla": False,
                "is_late_delivery": True,
                "has_complete_gps": True,
                "is_quality_warning": True,
                "delivery_count": 1,
            },
        ]
    )


def build_courier_daily() -> pd.DataFrame:
    """Construit un agrégat quotidien par coursier."""

    return pd.DataFrame(
        [
            {
                "delivery_date": (
                    "2000-09-25T00:00:00+00:00"
                ),
                "region_id": 31,
                "city": "Jilin",
                "courier_id": 100,
                "orders_total": 2,
                "orders_valid_duration": 2,
                "orders_within_sla": 1,
                "orders_late": 1,
                "avg_duration_minutes": 195.0,
                "sla_compliance_rate": 50.0,
                "gps_completeness_rate": 100.0,
                "quality_warning_rate": 50.0,
            }
        ]
    )


def build_city_daily() -> pd.DataFrame:
    """Construit un agrégat quotidien par ville."""

    return pd.DataFrame(
        [
            {
                "delivery_date": (
                    "2000-09-25T00:00:00+00:00"
                ),
                "region_id": 31,
                "city": "Jilin",
                "orders_total": 2,
                "orders_valid_duration": 2,
                "orders_within_sla": 1,
                "orders_late": 1,
                "unique_couriers": 1,
                "avg_duration_minutes": 195.0,
                "sla_compliance_rate": 50.0,
                "gps_completeness_rate": 100.0,
                "quality_warning_rate": 50.0,
            }
        ]
    )


def build_mock_loader() -> PostgresGoldLoader:
    """Construit un chargeur avec moteur simulé."""

    return PostgresGoldLoader(
        database_url=(
            "postgresql+psycopg://"
            "user:password@localhost:5433/database"
        ),
        schema_name="analytics",
        chunksize=100,
        engine=MagicMock(),
    )


def test_invalid_schema_identifier_is_rejected() -> None:
    with pytest.raises(
        ValueError,
        match="invalide",
    ):
        PostgresGoldLoader(
            database_url=(
                "postgresql+psycopg://"
                "user:password@localhost/database"
            ),
            schema_name="analytics;drop",
        )


def test_missing_required_column_is_rejected() -> None:
    loader = build_mock_loader()

    dataframe = build_delivery_fact().drop(
        columns=["order_id"]
    )

    with pytest.raises(
        PostgresGoldLoadError,
        match="obligatoires",
    ):
        loader.load_dataframe(
            dataframe=dataframe,
            table_name="delivery_fact",
        )


def test_duplicate_business_key_is_rejected() -> None:
    loader = build_mock_loader()

    dataframe = build_delivery_fact()

    dataframe.loc[
        1,
        "order_id",
    ] = 1

    with pytest.raises(
        PostgresGoldLoadError,
        match="doublons",
    ):
        loader.load_dataframe(
            dataframe=dataframe,
            table_name="delivery_fact",
        )


def test_unknown_table_is_rejected() -> None:
    loader = build_mock_loader()

    with pytest.raises(
        PostgresGoldLoadError,
        match="non prise en charge",
    ):
        loader.load_dataframe(
            dataframe=build_delivery_fact(),
            table_name="unknown_table",
        )


def test_copy_dataframe_uses_psycopg_copy() -> None:
    loader = build_mock_loader()
    dataframe = build_delivery_fact()

    connection = MagicMock()
    cursor = MagicMock()
    copy = MagicMock()

    connection.connection.cursor.return_value.__enter__.return_value = (
        cursor
    )

    cursor.copy.return_value.__enter__.return_value = (
        copy
    )

    loader._copy_dataframe(
        connection=connection,
        dataframe=dataframe,
        table_name="delivery_fact",
    )

    copy_sql = (
        cursor.copy.call_args.args[0]
    )

    payload = (
        copy.write.call_args.args[0]
    )

    assert (
        'COPY "analytics"."delivery_fact"'
        in copy_sql
    )

    assert "FORMAT CSV" in copy_sql

    assert isinstance(
        payload,
        bytes,
    )

    assert b"Jilin" in payload

    assert copy.write.call_count == 1


def test_write_dataframe_drops_indexes_before_copy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = build_mock_loader()
    dataframe = build_delivery_fact()

    calls: list[str] = []

    monkeypatch.setattr(
        loader,
        "_drop_indexes",
        lambda **kwargs: calls.append(
            "drop_indexes"
        ),
    )

    monkeypatch.setattr(
        loader,
        "_truncate_table",
        lambda **kwargs: calls.append(
            "truncate"
        ),
    )

    monkeypatch.setattr(
        loader,
        "_copy_dataframe",
        lambda **kwargs: calls.append(
            "copy"
        ),
    )

    loader._write_dataframe(
        connection=MagicMock(),
        dataframe=dataframe,
        table_name="delivery_fact",
        table_exists=True,
    )

    assert calls == [
        "drop_indexes",
        "truncate",
        "copy",
    ]


def test_load_dataframe_returns_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = build_mock_loader()
    dataframe = build_delivery_fact()

    connection = MagicMock()

    loader.engine.begin.return_value.__enter__.return_value = (
        connection
    )

    monkeypatch.setattr(
        loader,
        "_ensure_schema",
        lambda current_connection: None,
    )

    monkeypatch.setattr(
        loader,
        "_table_exists",
        lambda connection, table_name: False,
    )

    monkeypatch.setattr(
        loader,
        "_write_dataframe",
        lambda **kwargs: None,
    )

    monkeypatch.setattr(
        loader,
        "_count_rows",
        lambda **kwargs: len(dataframe),
    )

    monkeypatch.setattr(
        loader,
        "_create_indexes",
        lambda **kwargs: None,
    )

    monkeypatch.setattr(
        loader,
        "_analyze_table",
        lambda **kwargs: None,
    )

    result = loader.load_dataframe(
        dataframe=dataframe,
        table_name="delivery_fact",
        loaded_at=datetime(
            2026,
            8,
            3,
            20,
            0,
            tzinfo=UTC,
        ),
    )

    assert result.schema_name == "analytics"
    assert result.table_name == "delivery_fact"
    assert result.row_count == 2
    assert result.column_count == 12
    assert not result.replaced_existing_data


def test_load_gold_tables_returns_three_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = build_mock_loader()

    monkeypatch.setattr(
        loader,
        "test_connection",
        lambda: (
            "smartlogix",
            "smartlogix",
        ),
    )

    def fake_load_dataframe(
        dataframe: pd.DataFrame,
        table_name: str,
        loaded_at: datetime | None = None,
    ):
        from smartlogix.storage import (
            PostgresTableLoadResult,
        )

        return PostgresTableLoadResult(
            schema_name="analytics",
            table_name=table_name,
            row_count=len(dataframe),
            column_count=len(
                dataframe.columns
            ),
            replaced_existing_data=False,
            loaded_at=(
                loaded_at
                or datetime.now(UTC)
            ),
        )

    monkeypatch.setattr(
        loader,
        "load_dataframe",
        fake_load_dataframe,
    )

    result = loader.load_gold_tables(
        delivery_fact=build_delivery_fact(),
        courier_daily_performance=(
            build_courier_daily()
        ),
        city_daily_performance=(
            build_city_daily()
        ),
    )

    assert result.database_name == "smartlogix"
    assert result.database_user == "smartlogix"

    assert (
        result.delivery_fact.row_count
        == 2
    )

    assert (
        result.courier_daily_performance.row_count
        == 1
    )

    assert (
        result.city_daily_performance.row_count
        == 1
    )