import pytest

from smartlogix.ml.dataset import (
    MODEL_FEATURE_COLUMNS,
    build_dataset_sql,
    find_leakage_columns,
    validate_model_features,
)


def test_model_features_have_no_leakage() -> None:
    assert find_leakage_columns(MODEL_FEATURE_COLUMNS) == ()
    validate_model_features(MODEL_FEATURE_COLUMNS)


def test_leakage_detection_rejects_target() -> None:
    columns = ("city", "sla_minutes", "is_late_delivery")

    assert find_leakage_columns(columns) == ("is_late_delivery",)

    with pytest.raises(ValueError, match="is_late_delivery"):
        validate_model_features(columns)


def test_leakage_detection_rejects_post_delivery_features() -> None:
    columns = (
        "city",
        "delivery_duration_minutes",
        "is_within_sla",
    )

    leakage = find_leakage_columns(columns)

    assert leakage == (
        "delivery_duration_minutes",
        "is_within_sla",
    )


def test_dataset_sql_contains_ml_v1_contract() -> None:
    sql = build_dataset_sql(limit=5)

    assert "FROM analytics.delivery_fact" in sql
    assert "is_valid_duration = TRUE" in sql
    assert "2000-11-01 00:00:00+00" in sql
    assert "AS accept_weekday" in sql
    assert "AS accept_is_weekend" in sql
    assert "THEN 'night'" in sql
    assert "THEN 'morning'" in sql
    assert "THEN 'afternoon'" in sql
    assert "ELSE 'evening'" in sql
    assert "is_late_delivery" in sql
    assert sql.endswith("LIMIT 5;")


@pytest.mark.parametrize("limit", [0, -1, -100])
def test_dataset_sql_rejects_invalid_limit(limit: int) -> None:
    with pytest.raises(ValueError, match="strictly positive"):
        build_dataset_sql(limit=limit)