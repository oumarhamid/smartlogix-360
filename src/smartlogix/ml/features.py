from __future__ import annotations

import re

from smartlogix.ml.dataset import LEAKAGE_COLUMNS

SQL_ALIAS_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")

HISTORICAL_FEATURE_COLUMNS = (
    "courier_prev_day_orders_total",
    "courier_prev_day_orders_late",
    "courier_prev_day_avg_duration_minutes",
    "courier_prev_day_sla_compliance_rate",
    "courier_prev_day_quality_warning_rate",
    "courier_prev_day_available",
    "city_prev_day_orders_total",
    "city_prev_day_orders_late",
    "city_prev_day_unique_couriers",
    "city_prev_day_avg_duration_minutes",
    "city_prev_day_sla_compliance_rate",
    "city_prev_day_quality_warning_rate",
    "city_prev_day_available",
)


def validate_feature_contract() -> None:
    """Vérifie que les features historiques ne contiennent aucun leakage connu."""

    duplicated = len(HISTORICAL_FEATURE_COLUMNS) != len(set(HISTORICAL_FEATURE_COLUMNS))
    if duplicated:
        raise ValueError("Historical feature names must be unique")

    leakage = set(HISTORICAL_FEATURE_COLUMNS) & LEAKAGE_COLUMNS
    if leakage:
        joined = ", ".join(sorted(leakage))
        raise ValueError(f"Historical features contain leakage: {joined}")


def build_historical_feature_select_sql() -> str:
    """Retourne les expressions SQL des features historiques J-1."""

    return """
courier_prev.orders_total AS courier_prev_day_orders_total,
courier_prev.orders_late AS courier_prev_day_orders_late,
courier_prev.avg_duration_minutes AS courier_prev_day_avg_duration_minutes,
courier_prev.sla_compliance_rate AS courier_prev_day_sla_compliance_rate,
courier_prev.quality_warning_rate AS courier_prev_day_quality_warning_rate,
(courier_prev.courier_id IS NOT NULL) AS courier_prev_day_available,
city_prev.orders_total AS city_prev_day_orders_total,
city_prev.orders_late AS city_prev_day_orders_late,
city_prev.unique_couriers AS city_prev_day_unique_couriers,
city_prev.avg_duration_minutes AS city_prev_day_avg_duration_minutes,
city_prev.sla_compliance_rate AS city_prev_day_sla_compliance_rate,
city_prev.quality_warning_rate AS city_prev_day_quality_warning_rate,
(city_prev.city IS NOT NULL) AS city_prev_day_available
""".strip()


def build_historical_feature_join_sql(base_alias: str = "d") -> str:
    """Construit les jointures J-1 disponibles avant l'acceptation de la commande."""

    if not SQL_ALIAS_PATTERN.fullmatch(base_alias):
        raise ValueError("Invalid SQL base alias")

    return f"""
LEFT JOIN analytics.courier_daily_performance AS courier_prev
    ON courier_prev.region_id = {base_alias}.region_id
    AND courier_prev.city = {base_alias}.city
    AND courier_prev.courier_id = {base_alias}.courier_id
    AND (courier_prev.delivery_date AT TIME ZONE 'UTC')::date
        = ({base_alias}.accept_timestamp AT TIME ZONE 'UTC')::date - 1
LEFT JOIN analytics.city_daily_performance AS city_prev
    ON city_prev.region_id = {base_alias}.region_id
    AND city_prev.city = {base_alias}.city
    AND (city_prev.delivery_date AT TIME ZONE 'UTC')::date
        = ({base_alias}.accept_timestamp AT TIME ZONE 'UTC')::date - 1
""".strip()