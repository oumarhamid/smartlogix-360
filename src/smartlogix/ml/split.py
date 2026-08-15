from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

UTC = ZoneInfo("UTC")

TRAIN_END = datetime(2000, 10, 1, tzinfo=UTC)
VALIDATION_END = datetime(2000, 10, 15, tzinfo=UTC)
TEST_END = datetime(2000, 11, 1, tzinfo=UTC)

TRAIN_SPLIT = "train"
VALIDATION_SPLIT = "validation"
TEST_SPLIT = "test"
EXCLUDED_SPLIT = "excluded"

VALID_SPLITS = (
    TRAIN_SPLIT,
    VALIDATION_SPLIT,
    TEST_SPLIT,
)


def assign_temporal_split(accept_timestamp: datetime) -> str:
    """Affecte une livraison au split ML en respectant strictement le temps."""

    if accept_timestamp.tzinfo is None:
        raise ValueError("accept_timestamp must be timezone-aware")

    timestamp = accept_timestamp.astimezone(UTC)

    if timestamp < TRAIN_END:
        return TRAIN_SPLIT

    if timestamp < VALIDATION_END:
        return VALIDATION_SPLIT

    if timestamp < TEST_END:
        return TEST_SPLIT

    return EXCLUDED_SPLIT


def build_split_case_sql(column: str = "accept_timestamp") -> str:
    """Retourne le CASE SQL correspondant exactement au split Python."""

    if not column.replace("_", "").isalnum():
        raise ValueError("Invalid SQL column name")

    return f"""
CASE
    WHEN {column} < TIMESTAMPTZ '2000-10-01 00:00:00+00'
        THEN 'train'
    WHEN {column} < TIMESTAMPTZ '2000-10-15 00:00:00+00'
        THEN 'validation'
    WHEN {column} < TIMESTAMPTZ '2000-11-01 00:00:00+00'
        THEN 'test'
    ELSE 'excluded'
END
""".strip()