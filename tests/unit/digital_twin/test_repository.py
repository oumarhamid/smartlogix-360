from datetime import UTC, datetime

import pytest

from smartlogix.digital_twin.models import (
    TwinHistoryCoverage,
    TwinOperationalState,
)
from smartlogix.digital_twin.repository import (
    DigitalTwinRepository,
    snapshot_from_rows,
    twin_order_from_row,
)
from smartlogix.streaming.postgres_sink import (
    RealtimePostgresConfig,
)


def make_row(
    *,
    order_id: int = 697920,
    probability: float = 0.35,
    predicted_late: bool = True,
    alert_active: bool = True,
    courier_history: bool = True,
    city_history: bool = True,
) -> dict[str, object]:
    timestamp = datetime(
        2000,
        5,
        17,
        13,
        18,
        tzinfo=UTC,
    )

    return {
        "order_id": order_id,
        "source_event_time": timestamp,
        "region_id": 31,
        "city": "Jilin",
        "courier_id": 435,
        "aoi_id": 7753,
        "delay_probability": probability,
        "predicted_late": predicted_late,
        "threshold": 0.246906,
        "model_name": "lightgbm",
        "model_version": "lightgbm-delay-v1",
        "courier_prev_day_available": (
            courier_history
        ),
        "city_prev_day_available": city_history,
        "updated_at": timestamp,
        "alert_active": alert_active,
    }


def test_twin_order_from_row() -> None:
    order = twin_order_from_row(
        make_row()
    )

    assert order.order_id == 697920
    assert order.city == "Jilin"
    assert order.delay_probability == 0.35
    assert order.predicted_late is True
    assert order.alert_active is True

    assert (
        order.operational_state
        == TwinOperationalState.AT_RISK
    )

    assert (
        order.history_coverage
        == TwinHistoryCoverage.FULL
    )


def test_snapshot_from_rows() -> None:
    snapshot = snapshot_from_rows(
        [
            make_row(
                order_id=1,
                probability=0.60,
            ),
            make_row(
                order_id=2,
                probability=0.10,
                predicted_late=False,
                alert_active=False,
            ),
        ],
        generated_at=datetime.now(UTC),
    )

    assert snapshot.total_orders == 2
    assert snapshot.at_risk_orders == 1
    assert snapshot.active_alerts == 1
    assert snapshot.risk_rate == 0.5


def test_snapshot_preserves_history_coverage() -> None:
    snapshot = snapshot_from_rows(
        [
            make_row(
                order_id=1,
                courier_history=True,
                city_history=True,
            ),
            make_row(
                order_id=2,
                courier_history=False,
                city_history=True,
            ),
        ],
        generated_at=datetime.now(UTC),
    )

    assert snapshot.full_history_orders == 1

    assert (
        snapshot.orders[0].history_coverage
        == TwinHistoryCoverage.FULL
    )

    assert (
        snapshot.orders[1].history_coverage
        == TwinHistoryCoverage.PARTIAL
    )


def test_empty_rows_create_empty_snapshot() -> None:
    snapshot = snapshot_from_rows(
        [],
        generated_at=datetime.now(UTC),
    )

    assert snapshot.total_orders == 0
    assert snapshot.at_risk_orders == 0
    assert snapshot.active_alerts == 0
    assert snapshot.by_city() == ()


def test_repository_rejects_empty_model_version() -> None:
    config = RealtimePostgresConfig(
        host="localhost",
        port=5433,
        database="smartlogix",
        user="smartlogix",
        password="test",
        schema="realtime",
    )

    with pytest.raises(
        ValueError,
        match="model_version",
    ):
        DigitalTwinRepository(
            config=config,
            model_version=" ",
        )