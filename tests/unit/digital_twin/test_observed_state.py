from datetime import UTC, datetime

import pytest

from smartlogix.digital_twin.models import (
    DigitalTwinSnapshot,
    ObservedDeliveryState,
    PredictionOutcome,
    TwinOperationalState,
    TwinOrderState,
)
from smartlogix.digital_twin.repository import (
    observed_state_from_row,
    twin_order_from_row,
)


def make_observed(
    *,
    late: bool = False,
) -> ObservedDeliveryState:
    timestamp = datetime(
        2000,
        5,
        1,
        17,
        1,
        tzinfo=UTC,
    )

    return ObservedDeliveryState(
        event_id="observed-event-1",
        event_type="delivery_update",
        event_time=datetime.now(UTC),
        source_event_time=timestamp,
        aoi_type=1,
        delivery_duration_minutes=300.0 if late else 147.0,
        sla_minutes=240.0,
        is_within_sla=not late,
        is_late_delivery=late,
        is_quality_warning=False,
        accept_gps_lng=106.55283,
        accept_gps_lat=29.57863,
        delivery_gps_lng=106.55962,
        delivery_gps_lat=29.60532,
        kafka_partition=0,
        kafka_offset=4,
        kafka_timestamp=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )


def make_order(
    *,
    order_id: int,
    probability: float,
    observed: ObservedDeliveryState | None,
) -> TwinOrderState:
    threshold = 0.246906
    predicted_late = probability >= threshold

    return TwinOrderState(
        order_id=order_id,
        source_event_time=datetime(
            2000,
            5,
            1,
            14,
            34,
            tzinfo=UTC,
        ),
        region_id=155,
        city="Chongqing",
        courier_id=9,
        aoi_id=59412,
        delay_probability=probability,
        predicted_late=predicted_late,
        threshold=threshold,
        model_name="lightgbm",
        model_version="lightgbm-delay-v1",
        courier_prev_day_available=False,
        city_prev_day_available=False,
        alert_active=predicted_late,
        updated_at=datetime.now(UTC),
        observed_state=observed,
    )


@pytest.mark.parametrize(
    (
        "probability",
        "actual_late",
        "expected",
    ),
    [
        (
            0.80,
            True,
            PredictionOutcome.TRUE_POSITIVE,
        ),
        (
            0.10,
            False,
            PredictionOutcome.TRUE_NEGATIVE,
        ),
        (
            0.80,
            False,
            PredictionOutcome.FALSE_POSITIVE,
        ),
        (
            0.10,
            True,
            PredictionOutcome.FALSE_NEGATIVE,
        ),
    ],
)
def test_prediction_outcome_matrix(
    probability: float,
    actual_late: bool,
    expected: PredictionOutcome,
) -> None:
    order = make_order(
        order_id=1,
        probability=probability,
        observed=make_observed(
            late=actual_late
        ),
    )

    assert order.prediction_outcome == expected


def test_pending_prediction_without_observation() -> None:
    order = make_order(
        order_id=1,
        probability=0.80,
        observed=None,
    )

    assert (
        order.prediction_outcome
        == PredictionOutcome.PENDING_OBSERVATION
    )

    assert (
        order.operational_state
        == TwinOperationalState.AT_RISK
    )


def test_observed_result_overrides_predictive_state() -> None:
    order = make_order(
        order_id=1,
        probability=0.80,
        observed=make_observed(
            late=False
        ),
    )

    assert (
        order.operational_state
        == TwinOperationalState.DELIVERED_ON_TIME
    )

    assert (
        order.prediction_outcome
        == PredictionOutcome.FALSE_POSITIVE
    )


def test_observed_late_state() -> None:
    order = make_order(
        order_id=1,
        probability=0.10,
        observed=make_observed(
            late=True
        ),
    )

    assert (
        order.operational_state
        == TwinOperationalState.DELIVERED_LATE
    )

    assert (
        order.prediction_outcome
        == PredictionOutcome.FALSE_NEGATIVE
    )


def test_prediction_and_observation_keep_distinct_timestamps() -> None:
    order = make_order(
        order_id=53875,
        probability=0.1295,
        observed=make_observed(
            late=False
        ),
    )

    assert order.observed_state is not None

    assert (
        order.source_event_time
        != order.observed_state.source_event_time
    )

    assert order.source_event_time == datetime(
        2000,
        5,
        1,
        14,
        34,
        tzinfo=UTC,
    )

    assert (
        order.observed_state.source_event_time
        == datetime(
            2000,
            5,
            1,
            17,
            1,
            tzinfo=UTC,
        )
    )


def test_snapshot_confusion_matrix_metrics() -> None:
    snapshot = DigitalTwinSnapshot(
        generated_at=datetime.now(UTC),
        orders=(
            make_order(
                order_id=1,
                probability=0.80,
                observed=make_observed(
                    late=True
                ),
            ),
            make_order(
                order_id=2,
                probability=0.10,
                observed=make_observed(
                    late=False
                ),
            ),
            make_order(
                order_id=3,
                probability=0.80,
                observed=make_observed(
                    late=False
                ),
            ),
            make_order(
                order_id=4,
                probability=0.10,
                observed=make_observed(
                    late=True
                ),
            ),
            make_order(
                order_id=5,
                probability=0.80,
                observed=None,
            ),
        ),
    )

    assert snapshot.total_orders == 5
    assert snapshot.observed_orders == 4
    assert snapshot.evaluated_predictions == 4

    assert snapshot.true_positives == 1
    assert snapshot.true_negatives == 1
    assert snapshot.false_positives == 1
    assert snapshot.false_negatives == 1

    assert snapshot.prediction_accuracy == pytest.approx(
        0.5
    )

    assert snapshot.prediction_precision == pytest.approx(
        0.5
    )

    assert snapshot.prediction_recall == pytest.approx(
        0.5
    )


def test_observed_state_from_postgres_like_row() -> None:
    row = {
        "order_id": 53875,
        "source_event_time": datetime(
            2000,
            5,
            1,
            14,
            34,
            tzinfo=UTC,
        ),
        "region_id": 155,
        "city": "Chongqing",
        "courier_id": 9,
        "aoi_id": 59412,
        "delay_probability": 0.1295,
        "predicted_late": False,
        "threshold": 0.246906,
        "model_name": "lightgbm",
        "model_version": "lightgbm-delay-v1",
        "courier_prev_day_available": False,
        "city_prev_day_available": False,
        "alert_active": False,
        "updated_at": datetime.now(UTC),
        "observed_event_id": "event-53875",
        "observed_event_type": "delivery_update",
        "observed_event_time": datetime.now(UTC),
        "observed_source_event_time": datetime(
            2000,
            5,
            1,
            17,
            1,
            tzinfo=UTC,
        ),
        "observed_aoi_type": 1,
        "observed_delivery_duration_minutes": 147.0,
        "observed_sla_minutes": 240.0,
        "observed_is_within_sla": True,
        "observed_is_late_delivery": False,
        "observed_is_quality_warning": False,
        "observed_accept_gps_lng": 106.55283,
        "observed_accept_gps_lat": 29.57863,
        "observed_delivery_gps_lng": 106.55962,
        "observed_delivery_gps_lat": 29.60532,
        "observed_kafka_partition": 0,
        "observed_kafka_offset": 4,
        "observed_kafka_timestamp": datetime.now(UTC),
        "observed_updated_at": datetime.now(UTC),
    }

    observed = observed_state_from_row(row)

    assert observed is not None
    assert observed.event_type == "delivery_update"
    assert observed.delivery_duration_minutes == 147.0
    assert observed.sla_minutes == 240.0
    assert observed.is_late_delivery is False

    order = twin_order_from_row(row)

    assert (
        order.prediction_outcome
        == PredictionOutcome.TRUE_NEGATIVE
    )

    assert (
        order.operational_state
        == TwinOperationalState.DELIVERED_ON_TIME
    )


def test_missing_live_state_returns_none() -> None:
    row = {
        "observed_event_id": None,
    }

    assert observed_state_from_row(row) is None


def test_observed_state_rejects_inconsistent_sla_flags() -> None:
    timestamp = datetime.now(UTC)

    with pytest.raises(
        ValueError,
        match="incoherents",
    ):
        ObservedDeliveryState(
            event_id="event-invalid",
            event_type="delivery_update",
            event_time=timestamp,
            source_event_time=timestamp,
            aoi_type=1,
            delivery_duration_minutes=100.0,
            sla_minutes=240.0,
            is_within_sla=True,
            is_late_delivery=True,
            is_quality_warning=False,
            accept_gps_lng=None,
            accept_gps_lat=None,
            delivery_gps_lng=None,
            delivery_gps_lat=None,
            kafka_partition=0,
            kafka_offset=1,
            kafka_timestamp=timestamp,
            updated_at=timestamp,
        )