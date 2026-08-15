from __future__ import annotations

from collections.abc import Iterable

SOURCE_TABLE = "analytics.delivery_fact"

TARGET_COLUMN = "is_late_delivery"

IDENTIFIER_COLUMNS = (
    "order_id",
    "accept_timestamp",
)

RAW_FEATURE_COLUMNS = (
    "region_id",
    "city",
    "courier_id",
    "aoi_id",
    "aoi_type",
    "sla_minutes",
    "accept_gps_lng",
    "accept_gps_lat",
    "accept_gps_valid",
)

DERIVED_FEATURE_COLUMNS = (
    "accept_hour",
    "accept_weekday",
    "accept_month",
    "accept_day",
    "accept_is_weekend",
    "accept_period",
)

MODEL_FEATURE_COLUMNS = RAW_FEATURE_COLUMNS + DERIVED_FEATURE_COLUMNS

# Colonnes interdites comme features ML.
#
# Elles sont soit :
# - connues uniquement après T0 = accept_timestamp ;
# - dérivées du résultat final de la livraison ;
# - directement liées à la cible.
#
# Elles ne doivent donc jamais être utilisées comme variables prédictives.
LEAKAGE_COLUMNS = frozenset(
    {
        "delivery_timestamp",
        "delivery_hour",
        "delivery_duration_minutes",
        "delivery_duration_hours",
        "kpi_duration_minutes",
        "delivery_duration_status",
        "is_valid_duration",
        "is_within_sla",
        "is_late_delivery",
        "delivery_gps_lng",
        "delivery_gps_lat",
        "delivery_gps_valid",
        "has_complete_gps",
        "gps_quality_status",
        "is_quality_warning",
        "quality_warning_count",
    }
)

# Contrat d'éligibilité du dataset ML V1.
#
# - seules les livraisons ayant une durée valide sont utilisées ;
# - la seule observation postérieure à octobre 2000 est exclue ;
# - le split temporel est géré séparément dans smartlogix.ml.split.
ELIGIBILITY_SQL = """
d.is_valid_duration = TRUE
AND d.accept_timestamp < TIMESTAMPTZ '2000-11-01 00:00:00+00'
""".strip()


def find_leakage_columns(columns: Iterable[str]) -> tuple[str, ...]:
    """Retourne les colonnes interdites présentes dans une liste de features."""

    return tuple(sorted(set(columns) & LEAKAGE_COLUMNS))


def validate_model_features(columns: Iterable[str]) -> None:
    """Refuse toute liste de features contenant une fuite de données."""

    leakage = find_leakage_columns(columns)

    if leakage:
        joined = ", ".join(leakage)
        raise ValueError(f"Data leakage detected in model features: {joined}")


def build_base_select_sql() -> str:
    """Retourne les colonnes disponibles au moment T0."""

    return """
d.order_id,
d.accept_timestamp,
d.region_id,
d.city,
d.courier_id,
d.aoi_id,
d.aoi_type,
d.sla_minutes,
d.accept_gps_lng,
d.accept_gps_lat,
d.accept_gps_valid,
EXTRACT(HOUR FROM d.accept_timestamp)::smallint AS accept_hour,
EXTRACT(ISODOW FROM d.accept_timestamp)::smallint AS accept_weekday,
EXTRACT(MONTH FROM d.accept_timestamp)::smallint AS accept_month,
EXTRACT(DAY FROM d.accept_timestamp)::smallint AS accept_day,
(
    EXTRACT(ISODOW FROM d.accept_timestamp) IN (6, 7)
) AS accept_is_weekend,
CASE
    WHEN EXTRACT(HOUR FROM d.accept_timestamp) < 6 THEN 'night'
    WHEN EXTRACT(HOUR FROM d.accept_timestamp) < 12 THEN 'morning'
    WHEN EXTRACT(HOUR FROM d.accept_timestamp) < 18 THEN 'afternoon'
    ELSE 'evening'
END AS accept_period
""".strip()


def build_dataset_sql(limit: int | None = None) -> str:
    """Construit la requête reproductible du dataset ML V1 de base."""

    if limit is not None and limit <= 0:
        raise ValueError("limit must be strictly positive")

    limit_sql = f"\nLIMIT {limit}" if limit is not None else ""

    return f"""
SELECT
    {build_base_select_sql()},
    d.{TARGET_COLUMN}
FROM {SOURCE_TABLE} AS d
WHERE {ELIGIBILITY_SQL}
ORDER BY d.accept_timestamp, d.order_id{limit_sql};
""".strip()