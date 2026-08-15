import pytest

from smartlogix.ml.features import (
    HISTORICAL_FEATURE_COLUMNS,
    build_historical_feature_join_sql,
    build_historical_feature_select_sql,
    validate_feature_contract,
)


def test_historical_feature_contract_is_valid() -> None:
    validate_feature_contract()


def test_historical_feature_names_are_unique() -> None:
    assert len(HISTORICAL_FEATURE_COLUMNS) == len(set(HISTORICAL_FEATURE_COLUMNS))


def test_historical_feature_select_contains_every_feature() -> None:
    sql = build_historical_feature_select_sql()

    for feature in HISTORICAL_FEATURE_COLUMNS:
        assert feature in sql


def test_historical_join_uses_courier_daily_performance() -> None:
    sql = build_historical_feature_join_sql()

    assert "analytics.courier_daily_performance" in sql
    assert "courier_prev.courier_id = d.courier_id" in sql


def test_historical_join_uses_city_daily_performance() -> None:
    sql = build_historical_feature_join_sql()

    assert "analytics.city_daily_performance" in sql
    assert "city_prev.city = d.city" in sql


def test_historical_join_uses_previous_day_only() -> None:
    sql = build_historical_feature_join_sql()

    assert sql.count("accept_timestamp AT TIME ZONE 'UTC'") == 2
    assert sql.count("::date - 1") == 2


def test_historical_join_accepts_custom_alias() -> None:
    sql = build_historical_feature_join_sql("delivery")

    assert "delivery.region_id" in sql
    assert "delivery.accept_timestamp" in sql


@pytest.mark.parametrize(
    "alias",
    [
        "",
        "delivery fact",
        "d; DROP TABLE delivery_fact",
        "d.x",
        "123d",
    ],
)
def test_historical_join_rejects_invalid_alias(alias: str) -> None:
    with pytest.raises(ValueError, match="Invalid SQL base alias"):
        build_historical_feature_join_sql(alias)