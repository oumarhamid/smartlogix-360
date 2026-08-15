from __future__ import annotations

import pytest

from smartlogix.streaming.postgres_sink import (
    RealtimePostgresConfig,
    realtime_merge_statements,
    realtime_schema_statements,
)


def test_realtime_postgres_config_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_HOST", "postgres")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DATABASE", "smartlogix")
    monkeypatch.setenv("POSTGRES_USER", "smartlogix")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("REALTIME_POSTGRES_SCHEMA", "realtime")
    monkeypatch.setenv("STREAMING_QUERY_NAME", "delivery-stream")

    config = RealtimePostgresConfig.from_env()

    assert config.host == "postgres"
    assert config.port == 5432
    assert config.schema == "realtime"
    assert config.query_name == "delivery-stream"


def test_realtime_postgres_config_rejects_unsafe_schema() -> None:
    with pytest.raises(ValueError, match="Identifiant PostgreSQL invalide"):
        RealtimePostgresConfig(
            host="postgres",
            port=5432,
            database="smartlogix",
            user="smartlogix",
            password="secret",
            schema="realtime;drop schema public",
        )


def test_realtime_schema_is_idempotent_and_uses_unlogged_stage() -> None:
    ddl = "\n".join(realtime_schema_statements("realtime"))

    assert "CREATE SCHEMA IF NOT EXISTS" in ddl
    assert "delivery_events" in ddl
    assert "delivery_live_status" in ddl
    assert "CREATE UNLOGGED TABLE IF NOT EXISTS" in ddl
    assert "PRIMARY KEY (event_id)" in ddl
    assert "order_id BIGINT PRIMARY KEY" in ddl


def test_realtime_merge_statements_are_idempotent() -> None:
    count_stage, event_insert, live_upsert, delete_stage = realtime_merge_statements(
        "realtime"
    )

    assert "COUNT(*)" in count_stage
    assert "ON CONFLICT (event_id) DO NOTHING" in event_insert
    assert "ON CONFLICT (order_id) DO UPDATE" in live_upsert
    assert "EXCLUDED.event_time" in live_upsert
    assert "DELETE FROM" in delete_stage
