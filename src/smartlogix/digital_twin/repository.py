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
    ObservedDeliveryState,
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

    live = sql.SQL("{}.{}").format(
        sql.Identifier(schema),
        sql.Identifier("delivery_live_status"),
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
            ) AS alert_active,

            live.event_id
                AS observed_event_id,
            live.event_type
                AS observed_event_type,
            live.event_time
                AS observed_event_time,
            live.source_event_time
                AS observed_source_event_time,

            live.aoi_type
                AS observed_aoi_type,

            live.delivery_duration_minutes
                AS observed_delivery_duration_minutes,
            live.sla_minutes
                AS observed_sla_minutes,

            live.is_within_sla
                AS observed_is_within_sla,
            live.is_late_delivery
                AS observed_is_late_delivery,
            live.is_quality_warning
                AS observed_is_quality_warning,

            live.accept_gps_lng
                AS observed_accept_gps_lng,
            live.accept_gps_lat
                AS observed_accept_gps_lat,

            live.delivery_gps_lng
                AS observed_delivery_gps_lng,
            live.delivery_gps_lat
                AS observed_delivery_gps_lat,

            live.kafka_partition
                AS observed_kafka_partition,
            live.kafka_offset
                AS observed_kafka_offset,
            live.kafka_timestamp
                AS observed_kafka_timestamp,

            live.updated_at
                AS observed_updated_at

        FROM {} AS latest

        LEFT JOIN {} AS live
            ON live.order_id = latest.order_id

        WHERE latest.model_version = %s

        ORDER BY latest.order_id
        """
    ).format(
        alerts,
        latest,
        live,
    )


def _optional_float(
    value: Any,
) -> float | None:
    if value is None:
        return None

    return float(value)


def _optional_int(
    value: Any,
) -> int | None:
    if value is None:
        return None

    return int(value)


def _optional_bool(
    value: Any,
) -> bool | None:
    if value is None:
        return None

    return bool(value)


def observed_state_from_row(
    row: Mapping[str, Any],
) -> ObservedDeliveryState | None:
    event_id = row.get(
        "observed_event_id"
    )

    if event_id is None:
        return None

    return ObservedDeliveryState(
        event_id=str(event_id),
        event_type=str(
            row["observed_event_type"]
        ),
        event_time=row[
            "observed_event_time"
        ],
        source_event_time=row[
            "observed_source_event_time"
        ],
        aoi_type=_optional_int(
            row["observed_aoi_type"]
        ),
        delivery_duration_minutes=_optional_float(
            row[
                "observed_delivery_duration_minutes"
            ]
        ),
        sla_minutes=_optional_float(
            row["observed_sla_minutes"]
        ),
        is_within_sla=_optional_bool(
            row["observed_is_within_sla"]
        ),
        is_late_delivery=_optional_bool(
            row["observed_is_late_delivery"]
        ),
        is_quality_warning=_optional_bool(
            row["observed_is_quality_warning"]
        ),
        accept_gps_lng=_optional_float(
            row["observed_accept_gps_lng"]
        ),
        accept_gps_lat=_optional_float(
            row["observed_accept_gps_lat"]
        ),
        delivery_gps_lng=_optional_float(
            row["observed_delivery_gps_lng"]
        ),
        delivery_gps_lat=_optional_float(
            row["observed_delivery_gps_lat"]
        ),
        kafka_partition=_optional_int(
            row["observed_kafka_partition"]
        ),
        kafka_offset=_optional_int(
            row["observed_kafka_offset"]
        ),
        kafka_timestamp=row[
            "observed_kafka_timestamp"
        ],
        updated_at=row[
            "observed_updated_at"
        ],
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
        observed_state=observed_state_from_row(
            row
        ),
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

        with (
            psycopg.connect(
                **self.config.connection_kwargs(),
                row_factory=dict_row,
            ) as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute(
                query,
                (self.model_version,),
            )
            rows = cursor.fetchall()

        return snapshot_from_rows(rows)