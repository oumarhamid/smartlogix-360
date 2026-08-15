from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from smartlogix.digital_twin.models import (
    DigitalTwinSnapshot,
    TwinOrderState,
)
from smartlogix.ml.artifact import FINAL_MODEL_VERSION
from smartlogix.streaming.postgres_sink import (
    RealtimePostgresConfig,
)


def build_snapshot_query(
    schema: str,
) -> sql.Composed:
    latest = sql.SQL("{}.{}").format(
        sql.Identifier(schema),
        sql.Identifier("delivery_prediction_latest"),
    )

    alerts = sql.SQL("{}.{}").format(
        sql.Identifier(schema),
        sql.Identifier("delivery_alerts"),
    )

    return sql.SQL(
        """
        SELECT
            latest.order_id,
            latest.source_event_time,
            latest.region_id,
            latest.city,
            latest.courier_id,
            latest.aoi_id,
            latest.delay_probability,
            latest.predicted_late,
            latest.threshold,
            latest.model_name,
            latest.model_version,
            latest.courier_prev_day_available,
            latest.city_prev_day_available,
            latest.updated_at,
            EXISTS (
                SELECT 1
                FROM {} AS alert
                WHERE
                    alert.prediction_id =
                        latest.prediction_id
                    AND alert.model_version =
                        latest.model_version
                    AND alert.alert_type =
                        'predicted_delay_risk'
            ) AS alert_active
        FROM {} AS latest
        WHERE latest.model_version = %s
        ORDER BY latest.order_id
        """
    ).format(
        alerts,
        latest,
    )


def twin_order_from_row(
    row: Mapping[str, Any],
) -> TwinOrderState:
    return TwinOrderState(
        order_id=int(row["order_id"]),
        source_event_time=row["source_event_time"],
        region_id=int(row["region_id"]),
        city=str(row["city"]),
        courier_id=int(row["courier_id"]),
        aoi_id=int(row["aoi_id"]),
        delay_probability=float(
            row["delay_probability"]
        ),
        predicted_late=bool(
            row["predicted_late"]
        ),
        threshold=float(row["threshold"]),
        model_name=str(row["model_name"]),
        model_version=str(row["model_version"]),
        courier_prev_day_available=bool(
            row["courier_prev_day_available"]
        ),
        city_prev_day_available=bool(
            row["city_prev_day_available"]
        ),
        alert_active=bool(row["alert_active"]),
        updated_at=row["updated_at"],
    )


def snapshot_from_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    generated_at: datetime | None = None,
) -> DigitalTwinSnapshot:
    timestamp = generated_at or datetime.now(UTC)

    return DigitalTwinSnapshot(
        generated_at=timestamp,
        orders=tuple(
            twin_order_from_row(row)
            for row in rows
        ),
    )


@dataclass(frozen=True, slots=True)
class DigitalTwinRepository:
    config: RealtimePostgresConfig
    model_version: str = FINAL_MODEL_VERSION

    def __post_init__(self) -> None:
        if not self.model_version.strip():
            raise ValueError(
                "model_version ne peut pas etre vide."
            )

    def load_snapshot(
        self,
    ) -> DigitalTwinSnapshot:
        query = build_snapshot_query(
            self.config.schema
        )

        with psycopg.connect(
            **self.config.connection_kwargs(),
            row_factory=dict_row,
        ) as connection, connection.cursor() as cursor:
            cursor.execute(
                query,
                (self.model_version,),
            )
            rows = cursor.fetchall()

        return snapshot_from_rows(rows)