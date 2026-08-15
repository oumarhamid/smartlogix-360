from datetime import datetime

import pytest

from smartlogix.ml.split import (
    EXCLUDED_SPLIT,
    TEST_SPLIT,
    TRAIN_SPLIT,
    UTC,
    VALIDATION_SPLIT,
    assign_temporal_split,
    build_split_case_sql,
)


def test_temporal_split_before_october_is_train() -> None:
    timestamp = datetime(2000, 9, 30, 23, 59, 59, tzinfo=UTC)

    assert assign_temporal_split(timestamp) == TRAIN_SPLIT


def test_temporal_split_october_first_is_validation() -> None:
    timestamp = datetime(2000, 10, 1, tzinfo=UTC)

    assert assign_temporal_split(timestamp) == VALIDATION_SPLIT


def test_temporal_split_october_fifteenth_is_test() -> None:
    timestamp = datetime(2000, 10, 15, tzinfo=UTC)

    assert assign_temporal_split(timestamp) == TEST_SPLIT


def test_temporal_split_november_first_is_excluded() -> None:
    timestamp = datetime(2000, 11, 1, tzinfo=UTC)

    assert assign_temporal_split(timestamp) == EXCLUDED_SPLIT


def test_temporal_split_rejects_naive_datetime() -> None:
    timestamp = datetime(2000, 10, 10)

    with pytest.raises(ValueError, match="timezone-aware"):
        assign_temporal_split(timestamp)


def test_split_sql_matches_expected_boundaries() -> None:
    sql = build_split_case_sql()

    assert "2000-10-01 00:00:00+00" in sql
    assert "2000-10-15 00:00:00+00" in sql
    assert "2000-11-01 00:00:00+00" in sql
    assert "THEN 'train'" in sql
    assert "THEN 'validation'" in sql
    assert "THEN 'test'" in sql
    assert "ELSE 'excluded'" in sql


def test_split_sql_rejects_invalid_column_name() -> None:
    with pytest.raises(ValueError, match="Invalid SQL column name"):
        build_split_case_sql("accept_timestamp; DROP TABLE delivery_fact")