import pytest

from smartlogix.ml.dataset import MODEL_FEATURE_COLUMNS
from smartlogix.ml.features import HISTORICAL_FEATURE_COLUMNS
from smartlogix.ml.query import (
    ENRICHED_MODEL_FEATURE_COLUMNS,
    build_enriched_dataset_sql,
    build_realtime_feature_sql,
)


def test_enriched_feature_contract_combines_base_and_history() -> None:
    assert ENRICHED_MODEL_FEATURE_COLUMNS == (
        MODEL_FEATURE_COLUMNS
        + HISTORICAL_FEATURE_COLUMNS
    )


def test_enriched_features_are_unique() -> None:
    assert len(
        ENRICHED_MODEL_FEATURE_COLUMNS
    ) == len(
        set(ENRICHED_MODEL_FEATURE_COLUMNS)
    )


def test_enriched_dataset_contains_base_table() -> None:
    sql = build_enriched_dataset_sql(
        limit=10
    )

    assert (
        "FROM analytics.delivery_fact AS d"
        in sql
    )
    assert "is_valid_duration = TRUE" in sql


def test_enriched_dataset_contains_courier_history() -> None:
    sql = build_enriched_dataset_sql()

    assert (
        "analytics.courier_daily_performance"
        in sql
    )
    assert (
        "courier_prev_day_orders_total"
        in sql
    )
    assert (
        "courier_prev_day_sla_compliance_rate"
        in sql
    )


def test_enriched_dataset_contains_city_history() -> None:
    sql = build_enriched_dataset_sql()

    assert (
        "analytics.city_daily_performance"
        in sql
    )
    assert (
        "city_prev_day_orders_total"
        in sql
    )
    assert (
        "city_prev_day_sla_compliance_rate"
        in sql
    )


def test_enriched_dataset_uses_previous_day_only() -> None:
    sql = build_enriched_dataset_sql()

    assert sql.count(
        "::date - 1"
    ) == 2


def test_enriched_dataset_contains_target() -> None:
    sql = build_enriched_dataset_sql()

    assert "d.is_late_delivery" in sql


def test_enriched_dataset_limit() -> None:
    sql = build_enriched_dataset_sql(
        limit=25
    )

    assert sql.endswith(
        "LIMIT 25;"
    )


@pytest.mark.parametrize(
    "limit",
    [0, -1, -50],
)
def test_enriched_dataset_rejects_invalid_limit(
    limit: int,
) -> None:
    with pytest.raises(
        ValueError,
        match="strictly positive",
    ):
        build_enriched_dataset_sql(
            limit=limit
        )


def test_realtime_feature_query_uses_event_cte() -> None:
    sql = build_realtime_feature_sql()

    assert "WITH d AS" in sql
    assert "%(order_id)s" in sql
    assert "%(accept_timestamp)s" in sql
    assert "%(courier_id)s" in sql
    assert "%(aoi_id)s" in sql


def test_realtime_feature_query_contains_temporal_features() -> None:
    sql = build_realtime_feature_sql()

    assert "accept_hour" in sql
    assert "accept_weekday" in sql
    assert "accept_month" in sql
    assert "accept_day" in sql
    assert "accept_is_weekend" in sql
    assert "accept_period" in sql


def test_realtime_feature_query_contains_j1_history() -> None:
    sql = build_realtime_feature_sql()

    assert (
        "analytics.courier_daily_performance"
        in sql
    )
    assert (
        "analytics.city_daily_performance"
        in sql
    )

    assert sql.count(
        "::date - 1"
    ) == 2


def test_realtime_feature_query_has_no_target() -> None:
    sql = build_realtime_feature_sql()

    assert "d.is_late_delivery" not in sql


def test_realtime_feature_query_has_no_post_delivery_features() -> None:
    sql = build_realtime_feature_sql()

    forbidden = (
        "delivery_duration_minutes",
        "is_within_sla",
        "delivery_gps_lng",
        "delivery_gps_lat",
        "is_quality_warning",
    )

    for column in forbidden:
        assert column not in sql