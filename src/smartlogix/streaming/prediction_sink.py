from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import psycopg
from psycopg import sql

from smartlogix.streaming.postgres_sink import RealtimePostgresConfig


@dataclass(frozen=True, slots=True)
class PredictionPersistResult:
    prediction_inserted: bool
    latest_updated: bool
    alert_inserted: bool


def _qualified_name(
    schema: str,
    table: str,
) -> sql.Composed:
    return sql.SQL("{}.{}").format(
        sql.Identifier(schema),
        sql.Identifier(table),
    )


def _parse_timestamp(
    value: str | datetime,
) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        result = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )

    if result.tzinfo is None:
        return result.replace(tzinfo=UTC)

    return result.astimezone(UTC)


def prediction_schema_statements(
    schema: str,
) -> tuple[sql.Composable, ...]:
    predictions = _qualified_name(
        schema,
        "delivery_predictions",
    )
    latest = _qualified_name(
        schema,
        "delivery_prediction_latest",
    )
    alerts = _qualified_name(
        schema,
        "delivery_alerts",
    )

    return (
        sql.SQL(
            "CREATE SCHEMA IF NOT EXISTS {}"
        ).format(
            sql.Identifier(schema)
        ),
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {} (
                prediction_id TEXT PRIMARY KEY,
                source_event_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_time TIMESTAMPTZ NOT NULL,
                source_event_time TIMESTAMPTZ NOT NULL,

                order_id BIGINT NOT NULL
                    CHECK (order_id >= 0),

                region_id BIGINT NOT NULL
                    CHECK (region_id >= 0),

                city TEXT NOT NULL,

                courier_id BIGINT NOT NULL
                    CHECK (courier_id >= 0),

                aoi_id BIGINT NOT NULL
                    CHECK (aoi_id >= 0),

                delay_probability DOUBLE PRECISION NOT NULL
                    CHECK (
                        delay_probability >= 0
                        AND delay_probability <= 1
                    ),

                predicted_late BOOLEAN NOT NULL,

                threshold DOUBLE PRECISION NOT NULL
                    CHECK (
                        threshold >= 0
                        AND threshold <= 1
                    ),

                model_name TEXT NOT NULL,
                model_version TEXT NOT NULL,

                courier_prev_day_available BOOLEAN NOT NULL,
                city_prev_day_available BOOLEAN NOT NULL,

                ingested_at TIMESTAMPTZ
                    NOT NULL DEFAULT NOW(),

                UNIQUE (
                    source_event_id,
                    model_version
                )
            )
            """
        ).format(predictions),
        sql.SQL(
            """
            CREATE INDEX IF NOT EXISTS
                ix_realtime_predictions_order_time
            ON {} (
                order_id,
                source_event_time DESC
            )
            """
        ).format(predictions),
        sql.SQL(
            """
            CREATE INDEX IF NOT EXISTS
                ix_realtime_predictions_city_time
            ON {} (
                city,
                source_event_time DESC
            )
            """
        ).format(predictions),
        sql.SQL(
            """
            CREATE INDEX IF NOT EXISTS
                ix_realtime_predictions_late
            ON {} (
                source_event_time DESC
            )
            WHERE predicted_late
            """
        ).format(predictions),
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {} (
                order_id BIGINT NOT NULL
                    CHECK (order_id >= 0),

                model_version TEXT NOT NULL,

                prediction_id TEXT NOT NULL,
                source_event_id TEXT NOT NULL,
                event_time TIMESTAMPTZ NOT NULL,
                source_event_time TIMESTAMPTZ NOT NULL,

                region_id BIGINT NOT NULL
                    CHECK (region_id >= 0),

                city TEXT NOT NULL,

                courier_id BIGINT NOT NULL
                    CHECK (courier_id >= 0),

                aoi_id BIGINT NOT NULL
                    CHECK (aoi_id >= 0),

                delay_probability DOUBLE PRECISION NOT NULL
                    CHECK (
                        delay_probability >= 0
                        AND delay_probability <= 1
                    ),

                predicted_late BOOLEAN NOT NULL,

                threshold DOUBLE PRECISION NOT NULL
                    CHECK (
                        threshold >= 0
                        AND threshold <= 1
                    ),

                model_name TEXT NOT NULL,

                courier_prev_day_available BOOLEAN NOT NULL,
                city_prev_day_available BOOLEAN NOT NULL,

                updated_at TIMESTAMPTZ
                    NOT NULL DEFAULT NOW(),

                PRIMARY KEY (
                    order_id,
                    model_version
                )
            )
            """
        ).format(latest),
        sql.SQL(
            """
            CREATE INDEX IF NOT EXISTS
                ix_realtime_prediction_latest_city_risk
            ON {} (
                city,
                predicted_late,
                delay_probability DESC
            )
            """
        ).format(latest),
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {} (
                alert_id TEXT PRIMARY KEY,

                prediction_id TEXT NOT NULL,
                source_event_id TEXT NOT NULL,

                event_type TEXT NOT NULL,
                event_time TIMESTAMPTZ NOT NULL,
                source_event_time TIMESTAMPTZ NOT NULL,

                order_id BIGINT NOT NULL
                    CHECK (order_id >= 0),

                region_id BIGINT NOT NULL
                    CHECK (region_id >= 0),

                city TEXT NOT NULL,

                courier_id BIGINT NOT NULL
                    CHECK (courier_id >= 0),

                aoi_id BIGINT NOT NULL
                    CHECK (aoi_id >= 0),

                alert_type TEXT NOT NULL,
                severity TEXT NOT NULL,

                delay_probability DOUBLE PRECISION NOT NULL
                    CHECK (
                        delay_probability >= 0
                        AND delay_probability <= 1
                    ),

                threshold DOUBLE PRECISION NOT NULL
                    CHECK (
                        threshold >= 0
                        AND threshold <= 1
                    ),

                model_version TEXT NOT NULL,

                created_at TIMESTAMPTZ
                    NOT NULL DEFAULT NOW(),

                UNIQUE (
                    prediction_id,
                    alert_type
                )
            )
            """
        ).format(alerts),
        sql.SQL(
            """
            CREATE INDEX IF NOT EXISTS
                ix_realtime_alerts_city_time
            ON {} (
                city,
                source_event_time DESC
            )
            """
        ).format(alerts),
    )


def initialize_prediction_schema(
    config: RealtimePostgresConfig,
) -> None:
    with (
        psycopg.connect(
            **config.connection_kwargs()
        ) as connection,
        connection.cursor() as cursor,
    ):
        for statement in prediction_schema_statements(
            config.schema
        ):
            cursor.execute(statement)


def build_delay_alert(
    prediction: dict[str, Any],
) -> dict[str, Any] | None:
    if not bool(prediction["predicted_late"]):
        return None

    prediction_id = str(
        prediction["prediction_id"]
    )

    alert_id = str(
        uuid5(
            NAMESPACE_URL,
            (
                "smartlogix:"
                "delivery-delay-alert:"
                f"{prediction_id}"
            ),
        )
    )

    return {
        "alert_id": alert_id,
        "prediction_id": prediction_id,
        "source_event_id": prediction[
            "source_event_id"
        ],
        "event_type": "delivery_delay_alert",
        "event_time": prediction["event_time"],
        "source_event_time": prediction[
            "source_event_time"
        ],
        "order_id": prediction["order_id"],
        "region_id": prediction["region_id"],
        "city": prediction["city"],
        "courier_id": prediction["courier_id"],
        "aoi_id": prediction["aoi_id"],
        "alert_type": "predicted_delay_risk",
        "severity": "warning",
        "delay_probability": prediction[
            "delay_probability"
        ],
        "threshold": prediction["threshold"],
        "model_version": prediction[
            "model_version"
        ],
    }


def persist_prediction_and_alert(
    config: RealtimePostgresConfig,
    prediction: dict[str, Any],
    alert: dict[str, Any] | None,
) -> PredictionPersistResult:
    predictions = _qualified_name(
        config.schema,
        "delivery_predictions",
    )
    latest = _qualified_name(
        config.schema,
        "delivery_prediction_latest",
    )
    alerts = _qualified_name(
        config.schema,
        "delivery_alerts",
    )

    prediction_values = (
        prediction["prediction_id"],
        prediction["source_event_id"],
        prediction["event_type"],
        _parse_timestamp(
            prediction["event_time"]
        ),
        _parse_timestamp(
            prediction["source_event_time"]
        ),
        int(prediction["order_id"]),
        int(prediction["region_id"]),
        str(prediction["city"]),
        int(prediction["courier_id"]),
        int(prediction["aoi_id"]),
        float(prediction["delay_probability"]),
        bool(prediction["predicted_late"]),
        float(prediction["threshold"]),
        str(prediction["model_name"]),
        str(prediction["model_version"]),
        bool(
            prediction[
                "courier_prev_day_available"
            ]
        ),
        bool(
            prediction[
                "city_prev_day_available"
            ]
        ),
    )

    insert_prediction = sql.SQL(
        """
        INSERT INTO {} (
            prediction_id,
            source_event_id,
            event_type,
            event_time,
            source_event_time,
            order_id,
            region_id,
            city,
            courier_id,
            aoi_id,
            delay_probability,
            predicted_late,
            threshold,
            model_name,
            model_version,
            courier_prev_day_available,
            city_prev_day_available
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s
        )
        ON CONFLICT DO NOTHING
        RETURNING prediction_id
        """
    ).format(predictions)

    upsert_latest = sql.SQL(
        """
        INSERT INTO {} AS target (
            order_id,
            model_version,
            prediction_id,
            source_event_id,
            event_time,
            source_event_time,
            region_id,
            city,
            courier_id,
            aoi_id,
            delay_probability,
            predicted_late,
            threshold,
            model_name,
            courier_prev_day_available,
            city_prev_day_available
        )
        VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT (
            order_id,
            model_version
        )
        DO UPDATE SET
            prediction_id =
                EXCLUDED.prediction_id,
            source_event_id =
                EXCLUDED.source_event_id,
            event_time =
                EXCLUDED.event_time,
            source_event_time =
                EXCLUDED.source_event_time,
            region_id =
                EXCLUDED.region_id,
            city =
                EXCLUDED.city,
            courier_id =
                EXCLUDED.courier_id,
            aoi_id =
                EXCLUDED.aoi_id,
            delay_probability =
                EXCLUDED.delay_probability,
            predicted_late =
                EXCLUDED.predicted_late,
            threshold =
                EXCLUDED.threshold,
            model_name =
                EXCLUDED.model_name,
            courier_prev_day_available =
                EXCLUDED.courier_prev_day_available,
            city_prev_day_available =
                EXCLUDED.city_prev_day_available,
            updated_at = NOW()
        WHERE
            (
                EXCLUDED.source_event_time,
                EXCLUDED.prediction_id
            ) > (
                target.source_event_time,
                target.prediction_id
            )
        RETURNING prediction_id
        """
    ).format(latest)

    latest_values = (
        int(prediction["order_id"]),
        str(prediction["model_version"]),
        prediction["prediction_id"],
        prediction["source_event_id"],
        _parse_timestamp(
            prediction["event_time"]
        ),
        _parse_timestamp(
            prediction["source_event_time"]
        ),
        int(prediction["region_id"]),
        str(prediction["city"]),
        int(prediction["courier_id"]),
        int(prediction["aoi_id"]),
        float(prediction["delay_probability"]),
        bool(prediction["predicted_late"]),
        float(prediction["threshold"]),
        str(prediction["model_name"]),
        bool(
            prediction[
                "courier_prev_day_available"
            ]
        ),
        bool(
            prediction[
                "city_prev_day_available"
            ]
        ),
    )

    with (
        psycopg.connect(
            **config.connection_kwargs()
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(
            insert_prediction,
            prediction_values,
        )
        prediction_inserted = (
            cursor.fetchone() is not None
        )

        cursor.execute(
            upsert_latest,
            latest_values,
        )
        latest_updated = (
            cursor.fetchone() is not None
        )

        alert_inserted = False

        if alert is not None:
            insert_alert = sql.SQL(
                """
                INSERT INTO {} (
                    alert_id,
                    prediction_id,
                    source_event_id,
                    event_type,
                    event_time,
                    source_event_time,
                    order_id,
                    region_id,
                    city,
                    courier_id,
                    aoi_id,
                    alert_type,
                    severity,
                    delay_probability,
                    threshold,
                    model_version
                )
                VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                ON CONFLICT DO NOTHING
                RETURNING alert_id
                """
            ).format(alerts)

            cursor.execute(
                insert_alert,
                (
                    alert["alert_id"],
                    alert["prediction_id"],
                    alert["source_event_id"],
                    alert["event_type"],
                    _parse_timestamp(
                        alert["event_time"]
                    ),
                    _parse_timestamp(
                        alert["source_event_time"]
                    ),
                    int(alert["order_id"]),
                    int(alert["region_id"]),
                    str(alert["city"]),
                    int(alert["courier_id"]),
                    int(alert["aoi_id"]),
                    str(alert["alert_type"]),
                    str(alert["severity"]),
                    float(
                        alert[
                            "delay_probability"
                        ]
                    ),
                    float(alert["threshold"]),
                    str(alert["model_version"]),
                ),
            )

            alert_inserted = (
                cursor.fetchone() is not None
            )

    return PredictionPersistResult(
        prediction_inserted=prediction_inserted,
        latest_updated=latest_updated,
        alert_inserted=alert_inserted,
    )