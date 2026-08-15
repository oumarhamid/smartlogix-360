from __future__ import annotations

from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

DELIVERY_EVENT_COLUMNS = (
    "order_id",
    "region_id",
    "city",
    "courier_id",
    "aoi_id",
    "aoi_type",
    "delivery_timestamp",
    "delivery_duration_minutes",
    "sla_minutes",
    "is_within_sla",
    "is_late_delivery",
    "is_quality_warning",
    "accept_gps_lng",
    "accept_gps_lat",
    "delivery_gps_lng",
    "delivery_gps_lat",
)


def iter_parquet_records(
    parquet_path: Path,
    *,
    batch_size: int = 10_000,
) -> Iterator[dict[str, Any]]:
    """Lit un Parquet Gold par lots sans charger le fichier complet en RAM."""

    if batch_size <= 0:
        raise ValueError("batch_size doit etre strictement positif.")

    resolved_path = parquet_path.resolve()
    if not resolved_path.is_file():
        raise FileNotFoundError(f"Parquet introuvable: {resolved_path}")

    parquet_file = pq.ParquetFile(resolved_path)
    available_columns = set(parquet_file.schema_arrow.names)
    missing_columns = set(DELIVERY_EVENT_COLUMNS).difference(available_columns)

    if missing_columns:
        missing_text = ", ".join(sorted(missing_columns))
        raise ValueError(f"Colonnes Gold manquantes: {missing_text}")

    for record_batch in parquet_file.iter_batches(
        batch_size=batch_size,
        columns=list(DELIVERY_EVENT_COLUMNS),
    ):
        yield from record_batch.to_pylist()


def round_robin_records(
    sources: Mapping[str, Iterator[dict[str, Any]]],
) -> Iterator[dict[str, Any]]:
    """Entrelace les villes afin de simuler un flux logistique multi-ville."""

    active = list(sources.values())

    while active:
        next_active: list[Iterator[dict[str, Any]]] = []

        for source in active:
            try:
                yield next(source)
                next_active.append(source)
            except StopIteration:
                continue

        active = next_active
