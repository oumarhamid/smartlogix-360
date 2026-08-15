from datetime import UTC, datetime

import pytest

from smartlogix.digital_twin.models import (
    DigitalTwinSnapshot,
    TwinHistoryCoverage,
    TwinOperationalState,
    TwinOrderState,
)


def make_order(
    *,
    order_id: int = 1,
    city: str = "Jilin",
    probability: float = 0.30,
    threshold: float = 0.246906,
    courier_history: bool = True,
    city_history: bool = True,
    alert_active: bool | None = None,
) -> TwinOrderState:
    predicted_late = probability >= threshold

    if alert_active is None:
        alert_active = predicted_late

    timestamp = datetime(
        2000,
        5,
        17,
        13,
        18,
        tzinfo=UTC,
    )

    return TwinOrderState(
        order_id=order_id,
        source_event_time=timestamp,
        region_id=31,
        city=city,
        courier_id=435,
        aoi_id=7753,
        delay_probability=probability,
        predicted_late=predicted_late,
        threshold=threshold,
        model_name="lightgbm",
        model_version="lightgbm-delay-v1",
        courier_prev_day_available=courier_history,
        city_prev_day_available=city_history,
        alert_active=alert_active,
        updated_at=timestamp,
    )


def test_at_risk_operational_state() -> None:
    order = make_order(
        probability=0.60,
    )

    assert (
        order.operational_state
        == TwinOperationalState.AT_RISK
    )


def test_monitored_operational_state() -> None:
    order = make_order(
        probability=0.10,
        alert_active=False,
    )

    assert (
        order.operational_state
        == TwinOperationalState.MONITORED
    )


@pytest.mark.parametrize(
    (
        "courier_history",
        "city_history",
        "expected",
    ),
    [
        (
            True,
            True,
            TwinHistoryCoverage.FULL,
        ),
        (
            True,
            False,
            TwinHistoryCoverage.PARTIAL,
        ),
        (
            False,
            True,
            TwinHistoryCoverage.PARTIAL,
        ),
        (
            False,
            False,
            TwinHistoryCoverage.NONE,
        ),
    ],
)
def test_history_coverage(
    courier_history: bool,
    city_history: bool,
    expected: TwinHistoryCoverage,
) -> None:
    order = make_order(
        courier_history=courier_history,
        city_history=city_history,
    )

    assert order.history_coverage == expected


def test_rejects_probability_outside_range() -> None:
    with pytest.raises(
        ValueError,
        match="delay_probability",
    ):
        make_order(
            probability=1.2,
        )


def test_rejects_prediction_threshold_inconsistency() -> None:
    timestamp = datetime.now(UTC)

    with pytest.raises(
        ValueError,
        match="predicted_late",
    ):
        TwinOrderState(
            order_id=1,
            source_event_time=timestamp,
            region_id=31,
            city="Jilin",
            courier_id=435,
            aoi_id=7753,
            delay_probability=0.80,
            predicted_late=False,
            threshold=0.246906,
            model_name="lightgbm",
            model_version="lightgbm-delay-v1",
            courier_prev_day_available=True,
            city_prev_day_available=True,
            alert_active=False,
            updated_at=timestamp,
        )


def test_rejects_alert_without_risk() -> None:
    with pytest.raises(
        ValueError,
        match="alerte active",
    ):
        make_order(
            probability=0.10,
            alert_active=True,
        )


def test_snapshot_metrics() -> None:
    snapshot = DigitalTwinSnapshot(
        generated_at=datetime.now(UTC),
        orders=(
            make_order(
                order_id=1,
                city="Jilin",
                probability=0.60,
            ),
            make_order(
                order_id=2,
                city="Jilin",
                probability=0.10,
                alert_active=False,
            ),
            make_order(
                order_id=3,
                city="Yantai",
                probability=0.40,
                courier_history=False,
                city_history=False,
            ),
        ),
    )

    assert snapshot.total_orders == 3
    assert snapshot.at_risk_orders == 2
    assert snapshot.active_alerts == 2
    assert snapshot.risk_rate == pytest.approx(
        2 / 3
    )
    assert (
        snapshot.average_delay_probability
        == pytest.approx(0.3666666667)
    )
    assert (
        snapshot.maximum_delay_probability
        == pytest.approx(0.60)
    )
    assert snapshot.full_history_orders == 2


def test_snapshot_city_aggregation() -> None:
    snapshot = DigitalTwinSnapshot(
        generated_at=datetime.now(UTC),
        orders=(
            make_order(
                order_id=1,
                city="Jilin",
                probability=0.60,
            ),
            make_order(
                order_id=2,
                city="Jilin",
                probability=0.10,
                alert_active=False,
            ),
            make_order(
                order_id=3,
                city="Yantai",
                probability=0.40,
            ),
        ),
    )

    cities = {
        item.city: item
        for item in snapshot.by_city()
    }

    assert cities["Jilin"].total_orders == 2
    assert cities["Jilin"].at_risk_orders == 1
    assert cities["Jilin"].risk_rate == 0.5
    assert (
        cities["Jilin"].average_delay_probability
        == pytest.approx(0.35)
    )

    assert cities["Yantai"].total_orders == 1
    assert cities["Yantai"].at_risk_orders == 1


def test_snapshot_rejects_duplicate_order_model() -> None:
    order = make_order(
        order_id=42,
    )

    with pytest.raises(
        ValueError,
        match="dupliquees",
    ):
        DigitalTwinSnapshot(
            generated_at=datetime.now(UTC),
            orders=(
                order,
                order,
            ),
        )


def test_empty_snapshot_metrics() -> None:
    snapshot = DigitalTwinSnapshot(
        generated_at=datetime.now(UTC),
        orders=(),
    )

    assert snapshot.total_orders == 0
    assert snapshot.at_risk_orders == 0
    assert snapshot.active_alerts == 0
    assert snapshot.risk_rate == 0.0
    assert snapshot.average_delay_probability == 0.0
    assert snapshot.maximum_delay_probability == 0.0
    assert snapshot.by_city() == ()