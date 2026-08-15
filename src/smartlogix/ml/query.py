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
    """Construit le SELECT enrichi d'entraînement sans tri ni limite."""

    return f"""
SELECT
    {build_base_select_sql()},
    {build_historical_feature_select_sql()},
    d.{TARGET_COLUMN}
FROM {SOURCE_TABLE} AS d
{build_historical_feature_join_sql("d")}
WHERE {ELIGIBILITY_SQL}
""".strip()


def build_enriched_dataset_sql(
    limit: int | None = None,
) -> str:
    """Construit le dataset ML enrichi avec les features historiques J-1."""

    if limit is not None and limit <= 0:
        raise ValueError(
            "limit must be strictly positive"
        )

    limit_sql = (
        f"\nLIMIT {limit}"
        if limit is not None
        else ""
    )

    return f"""
{build_enriched_select_sql()}
ORDER BY d.accept_timestamp, d.order_id{limit_sql};
""".strip()


def build_realtime_feature_sql() -> str:
    """
    Construit la requête d'enrichissement d'un événement T0.

    La ligne d'entrée provient d'un delivery_accepted et est exposée
    sous l'alias ``d`` afin de réutiliser exactement les mêmes
    expressions temporelles et jointures historiques J-1 que lors
    de l'entraînement.

    Aucun champ post-livraison ni cible ML n'est utilisé.
    """

    return f"""
WITH d AS (
    SELECT
        %(order_id)s::bigint AS order_id,
        %(accept_timestamp)s::timestamptz AS accept_timestamp,
        %(region_id)s::bigint AS region_id,
        %(city)s::text AS city,
        %(courier_id)s::bigint AS courier_id,
        %(aoi_id)s::bigint AS aoi_id,
        %(aoi_type)s::integer AS aoi_type,
        %(sla_minutes)s::double precision AS sla_minutes,
        %(accept_gps_lng)s::double precision AS accept_gps_lng,
        %(accept_gps_lat)s::double precision AS accept_gps_lat,
        %(accept_gps_valid)s::boolean AS accept_gps_valid
)
SELECT
    {build_base_select_sql()},
    {build_historical_feature_select_sql()}
FROM d
{build_historical_feature_join_sql("d")};
""".strip()