from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from typing import Any

import psycopg

UTC = timezone.utc  # noqa: UP017

POSTGRES_IDENTIFIER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")

EVENT_COLUMNS = (
    "event_id",
    "event_type",
    "event_time",
    "source_event_time",
    "order_id",
    "region_id",
    "city",
    "courier_id",
    "aoi_id",
    "aoi_type",
    "delivery_duration_minutes",
    "sla_minutes",
    "is_within_sla",
    "is_late_delivery",
    "is_quality_warning",
    "accept_gps_lng",
    "accept_gps_lat",
    "delivery_gps_lng",
    "delivery_gps_lat",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
)


@dataclass(frozen=True, slots=True)
class RealtimePostgresConfig:
    """Configuration PostgreSQL du sink Structured Streaming."""

    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str = "realtime"
    query_name: str = "smartlogix-delivery-postgres"

    def __post_init__(self) -> None:
        if not self.host.strip():
            raise ValueError("POSTGRES_HOST est obligatoire.")
        if self.port <= 0:
            raise ValueError("POSTGRES_PORT doit etre strictement positif.")
        if not self.database.strip():
            raise ValueError("POSTGRES_DATABASE est obligatoire.")
        if not self.user.strip():
            raise ValueError("POSTGRES_USER est obligatoire.")
        if not self.password:
            raise ValueError("POSTGRES_PASSWORD est obligatoire.")
        _validate_identifier(self.schema)
        if not self.query_name.strip():
            raise ValueError("STREAMING_QUERY_NAME est obligatoire.")

    @classmethod
    def from_env(cls) -> RealtimePostgresConfig:
        """Construit la configuration depuis les variables d'environnement."""

        return cls(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DATABASE", "smartlogix"),
            user=os.getenv("POSTGRES_USER", "smartlogix"),
            password=os.getenv("POSTGRES_PASSWORD", "change_me"),
            schema=os.getenv("REALTIME_POSTGRES_SCHEMA", "realtime"),
            query_name=os.getenv(
                "STREAMING_QUERY_NAME",
                "smartlogix-delivery-postgres",
            ),
        )

    def connection_kwargs(self) -> dict[str, Any]:
        """Retourne les arguments de connexion psycopg."""

        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.user,
            "password": self.password,
            "connect_timeout": 10,
            "application_name": "smartlogix-streaming",
            "options": "-c timezone=UTC",
        }


def _validate_identifier(identifier: str) -> None:
    if not POSTGRES_IDENTIFIER_PATTERN.fullmatch(identifier):
        raise ValueError(f"Identifiant PostgreSQL invalide : {identifier!r}")


def _quote_identifier(identifier: str) -> str:
    _validate_identifier(identifier)
    return f'"{identifier}"'


def _qualified_name(schema: str, table: str) -> str:
    return f"{_quote_identifier(schema)}.{_quote_identifier(table)}"


def realtime_schema_statements(schema: str) -> tuple[str, ...]:
    """Retourne le DDL idempotent de la couche PostgreSQL temps reel."""

    _validate_identifier(schema)
    q_schema = _quote_identifier(schema)
    events = _qualified_name(schema, "delivery_events")
    live = _qualified_name(schema, "delivery_live_status")
    stage = _qualified_name(schema, "delivery_events_stage")

    common_columns = """
        event_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        event_time TIMESTAMPTZ NOT NULL,
        source_event_time TIMESTAMPTZ NOT NULL,
        order_id BIGINT NOT NULL CHECK (order_id >= 0),
        region_id BIGINT NOT NULL CHECK (region_id >= 0),
        city TEXT NOT NULL,
        courier_id BIGINT NOT NULL CHECK (courier_id >= 0),
        aoi_id BIGINT NOT NULL CHECK (aoi_id >= 0),
        aoi_type INTEGER NOT NULL CHECK (aoi_type >= 0),
        delivery_duration_minutes DOUBLE PRECISION,
        sla_minutes DOUBLE PRECISION NOT NULL CHECK (sla_minutes > 0),
        is_within_sla BOOLEAN NOT NULL,
        is_late_delivery BOOLEAN NOT NULL,
        is_quality_warning BOOLEAN NOT NULL,
        accept_gps_lng DOUBLE PRECISION,
        accept_gps_lat DOUBLE PRECISION,
        delivery_gps_lng DOUBLE PRECISION,
        delivery_gps_lat DOUBLE PRECISION,
        kafka_partition INTEGER NOT NULL,
        kafka_offset BIGINT NOT NULL,
        kafka_timestamp TIMESTAMPTZ NOT NULL
    """.strip()

    return (
        f"CREATE SCHEMA IF NOT EXISTS {q_schema}",
        f"""
        CREATE TABLE IF NOT EXISTS {events} (
            {common_columns},
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (event_id)
        )
        """.strip(),
        f"""
        CREATE INDEX IF NOT EXISTS ix_realtime_events_order_time
        ON {events} (order_id, event_time DESC)
        """.strip(),
        f"""
        CREATE INDEX IF NOT EXISTS ix_realtime_events_city_time
        ON {events} (city, event_time DESC)
        """.strip(),
        f"""
        CREATE INDEX IF NOT EXISTS ix_realtime_events_late_time
        ON {events} (event_time DESC)
        WHERE is_late_delivery
        """.strip(),
        f"""
        CREATE TABLE IF NOT EXISTS {live} (
            order_id BIGINT PRIMARY KEY CHECK (order_id >= 0),
            event_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            event_time TIMESTAMPTZ NOT NULL,
            source_event_time TIMESTAMPTZ NOT NULL,
            region_id BIGINT NOT NULL CHECK (region_id >= 0),
            city TEXT NOT NULL,
            courier_id BIGINT NOT NULL CHECK (courier_id >= 0),
            aoi_id BIGINT NOT NULL CHECK (aoi_id >= 0),
            aoi_type INTEGER NOT NULL CHECK (aoi_type >= 0),
            delivery_duration_minutes DOUBLE PRECISION,
            sla_minutes DOUBLE PRECISION NOT NULL CHECK (sla_minutes > 0),
            is_within_sla BOOLEAN NOT NULL,
            is_late_delivery BOOLEAN NOT NULL,
            is_quality_warning BOOLEAN NOT NULL,
            accept_gps_lng DOUBLE PRECISION,
            accept_gps_lat DOUBLE PRECISION,
            delivery_gps_lng DOUBLE PRECISION,
            delivery_gps_lat DOUBLE PRECISION,
            kafka_partition INTEGER NOT NULL,
            kafka_offset BIGINT NOT NULL,
            kafka_timestamp TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """.strip(),
        f"""
        CREATE INDEX IF NOT EXISTS ix_realtime_live_city_late
        ON {live} (city, is_late_delivery)
        """.strip(),
        f"""
        CREATE UNLOGGED TABLE IF NOT EXISTS {stage} (
            query_name TEXT NOT NULL,
            batch_id BIGINT NOT NULL,
            {common_columns},
            staged_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """.strip(),
        f"""
        CREATE INDEX IF NOT EXISTS ix_realtime_stage_query_batch
        ON {stage} (query_name, batch_id)
        """.strip(),
    )


def initialize_realtime_schema(config: RealtimePostgresConfig) -> None:
    """Cree le schema et les tables temps reel de facon idempotente."""

    with (
        psycopg.connect(**config.connection_kwargs()) as connection,
        connection.cursor() as cursor,
    ):
        for statement in realtime_schema_statements(config.schema):
            cursor.execute(statement)


def _copy_sql(schema: str) -> str:
    stage = _qualified_name(schema, "delivery_events_stage")
    columns = ("query_name", "batch_id", *EVENT_COLUMNS)
    quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
    return f"COPY {stage} ({quoted_columns}) FROM STDIN"


def _normalize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    return value


def _stage_partition(
    rows: Any,
    *,
    config: RealtimePostgresConfig,
    batch_id: int,
) -> None:
    """Charge une partition Spark dans la table de staging via COPY."""

    with (
        psycopg.connect(**config.connection_kwargs()) as connection,
        connection.cursor() as cursor,
        cursor.copy(_copy_sql(config.schema)) as copy,
    ):
        for row in rows:
            values = tuple(
                _normalize_value(row[column])
                for column in EVENT_COLUMNS
            )
            copy.write_row((config.query_name, batch_id, *values))


def realtime_merge_statements(schema: str) -> tuple[str, str, str, str]:
    """Retourne les requetes de fusion transactionnelle d'un micro-batch."""

    events = _qualified_name(schema, "delivery_events")
    live = _qualified_name(schema, "delivery_live_status")
    stage = _qualified_name(schema, "delivery_events_stage")
    quoted_event_columns = ", ".join(_quote_identifier(column) for column in EVENT_COLUMNS)

    event_insert = f"""
        WITH source AS (
            SELECT DISTINCT ON (event_id)
                {quoted_event_columns}
            FROM {stage}
            WHERE query_name = %s AND batch_id = %s
            ORDER BY
                event_id,
                event_time DESC,
                kafka_timestamp DESC,
                kafka_partition DESC,
                kafka_offset DESC
        )
        INSERT INTO {events} ({quoted_event_columns})
        SELECT {quoted_event_columns}
        FROM source
        ON CONFLICT (event_id) DO NOTHING
    """.strip()

    live_columns = tuple(column for column in EVENT_COLUMNS if column != "order_id")
    live_insert_columns = ("order_id", *live_columns)
    quoted_live_columns = ", ".join(
        _quote_identifier(column) for column in live_insert_columns
    )
    update_assignments = ",\n                ".join(
        f"{_quote_identifier(column)} = EXCLUDED.{_quote_identifier(column)}"
        for column in live_columns
    )

    live_upsert = f"""
        WITH source AS (
            SELECT DISTINCT ON (order_id)
                {quoted_event_columns}
            FROM {stage}
            WHERE query_name = %s AND batch_id = %s
            ORDER BY
                order_id,
                event_time DESC,
                kafka_timestamp DESC,
                event_id DESC
        )
        INSERT INTO {live} AS target ({quoted_live_columns})
        SELECT {quoted_live_columns}
        FROM source
        ON CONFLICT (order_id) DO UPDATE
        SET
                {update_assignments},
                updated_at = NOW()
        WHERE
            (
                EXCLUDED.event_time,
                EXCLUDED.kafka_timestamp,
                EXCLUDED.event_id
            ) > (
                target.event_time,
                target.kafka_timestamp,
                target.event_id
            )
    """.strip()

    count_stage = f"""
        SELECT COUNT(*)
        FROM {stage}
        WHERE query_name = %s AND batch_id = %s
    """.strip()

    delete_stage = f"""
        DELETE FROM {stage}
        WHERE query_name = %s AND batch_id = %s
    """.strip()

    return count_stage, event_insert, live_upsert, delete_stage


def _merge_staged_batch(
    config: RealtimePostgresConfig,
    batch_id: int,
) -> tuple[int, int, int]:
    count_stage, event_insert, live_upsert, delete_stage = realtime_merge_statements(
        config.schema
    )
    params = (config.query_name, batch_id)

    with (
        psycopg.connect(**config.connection_kwargs()) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(count_stage, params)
        staged_rows = int(cursor.fetchone()[0])

        cursor.execute(event_insert, params)
        inserted_events = max(cursor.rowcount, 0)

        cursor.execute(live_upsert, params)
        updated_live_rows = max(cursor.rowcount, 0)

        cursor.execute(delete_stage, params)

    return staged_rows, inserted_events, updated_live_rows


def write_realtime_microbatch(
    batch_dataframe: Any,
    batch_id: int,
    config: RealtimePostgresConfig,
) -> None:
    """Ecrit un micro-batch Spark dans PostgreSQL de maniere idempotente."""

    stage_writer = partial(
        _stage_partition,
        config=config,
        batch_id=int(batch_id),
    )
    batch_dataframe.foreachPartition(stage_writer)

    staged_rows, inserted_events, updated_live_rows = _merge_staged_batch(
        config,
        int(batch_id),
    )

    print(
        "SmartLogix PostgreSQL micro-batch "
        f"{batch_id}: staged={staged_rows}, "
        f"events_inserted={inserted_events}, "
        f"live_upserted={updated_live_rows}"
    )
