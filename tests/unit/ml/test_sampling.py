import pytest

from smartlogix.ml.sampling import (
    HASH_BUCKETS,
    LOCAL_SAMPLE_BUCKETS,
    build_hash_sample_predicate,
    build_local_sample_sql,
    validate_sample_split,
)


@pytest.mark.parametrize(
    "split",
    ["train", "validation", "test"],
)
def test_validate_sample_split_accepts_known_splits(split: str) -> None:
    assert validate_sample_split(split) == split


def test_validate_sample_split_rejects_unknown_split() -> None:
    with pytest.raises(ValueError, match="Unknown ML split"):
        validate_sample_split("unknown")


def test_local_sample_rates_are_valid() -> None:
    for bucket_count in LOCAL_SAMPLE_BUCKETS.values():
        assert 1 <= bucket_count <= HASH_BUCKETS


def test_train_sample_uses_train_temporal_boundary() -> None:
    sql = build_local_sample_sql("train")

    assert "2000-10-01" in sql
    assert "ml.accept_timestamp <" in sql


def test_validation_sample_uses_validation_boundaries() -> None:
    sql = build_local_sample_sql("validation")

    assert "2000-10-01" in sql
    assert "2000-10-15" in sql


def test_test_sample_uses_test_boundaries() -> None:
    sql = build_local_sample_sql("test")

    assert "2000-10-15" in sql
    assert "2000-11-01" in sql


@pytest.mark.parametrize(
    ("split", "bucket_count"),
    [
        ("train", 8),
        ("validation", 26),
        ("test", 20),
    ],
)
def test_sample_uses_expected_hash_bucket(
    split: str,
    bucket_count: int,
) -> None:
    sql = build_local_sample_sql(split)

    assert f") < {bucket_count}" in sql


def test_sample_is_deterministic() -> None:
    first = build_local_sample_sql("train")
    second = build_local_sample_sql("train")

    assert first == second
    assert "md5(" in first
    assert "smartlogix-ml-v1" in first


def test_sample_contains_enriched_features() -> None:
    sql = build_local_sample_sql("train")

    assert "analytics.delivery_fact AS d" in sql
    assert "analytics.courier_daily_performance" in sql
    assert "analytics.city_daily_performance" in sql
    assert "courier_prev_day_orders_total" in sql
    assert "city_prev_day_orders_total" in sql
    assert "is_late_delivery" in sql


@pytest.mark.parametrize("bucket_count", [0, -1, 257])
def test_hash_sample_rejects_invalid_bucket_count(
    bucket_count: int,
) -> None:
    with pytest.raises(ValueError, match="between"):
        build_hash_sample_predicate(bucket_count=bucket_count)