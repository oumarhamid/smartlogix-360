from __future__ import annotations

from smartlogix.ml.dataset import (
    ELIGIBILITY_SQL,
    MODEL_FEATURE_COLUMNS,
    SOURCE_TABLE,
    TARGET_COLUMN,
    build_base_select_sql,
)
from smartlogix.ml.features import (
    HISTORICAL_FEATURE_COLUMNS,
    build_historical_feature_join_sql,
    build_historical_feature_select_sql,
)

ENRICHED_MODEL_FEATURE_COLUMNS = (
    MODEL_FEATURE_COLUMNS + HISTORICAL_FEATURE_COLUMNS
)


def build_enriched_select_sql() -> str:
    """Construit le SELECT enrichi sans tri ni limite."""

    return f"""
SELECT
    {build_base_select_sql()},
    {build_historical_feature_select_sql()},
    d.{TARGET_COLUMN}
FROM {SOURCE_TABLE} AS d
{build_historical_feature_join_sql("d")}
WHERE {ELIGIBILITY_SQL}
""".strip()


def build_enriched_dataset_sql(limit: int | None = None) -> str:
    """Construit le dataset ML enrichi avec les features historiques J-1."""

    if limit is not None and limit <= 0:
        raise ValueError("limit must be strictly positive")

    limit_sql = f"\nLIMIT {limit}" if limit is not None else ""

    return f"""
{build_enriched_select_sql()}
ORDER BY d.accept_timestamp, d.order_id{limit_sql};
""".strip()