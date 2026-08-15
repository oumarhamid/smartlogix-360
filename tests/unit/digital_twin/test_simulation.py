from datetime import UTC, datetime

import pytest

from smartlogix.digital_twin.models import (
    DigitalTwinSnapshot,
    TwinOrderState,
)
from smartlogix.digital_twin.simulation import (
    SimulationEngine,
    SimulationScenario,
)


def make_order(
    *,
    order_id: int,
    city: str = "Jilin",
    probability: float = 0.20,
) -> TwinOrderState:
    timestamp = datetime(
        2000,
        5,
        17,
        13,
        18,
        tzinfo=UTC,
    )

    threshold = 0.246906

    return TwinOrderState(
        order_id=order_id,
        source_event_time=timestamp,
        region_id=31,
        city=city,
        courier_id=435,
        aoi_id=7753,
        delay_probability=probability,
        predicted_late=(
            probability >= threshold
        ),
        threshold=threshold,
        model_name="lightgbm",
        model_version="lightgbm-delay-v1",
        courier_prev_day_available=True,
        city_prev_day_available=True,
        alert_active=(
            probability >= threshold
        ),
        updated_at=timestamp,
    )


def make_snapshot() -> DigitalTwinSnapshot:
    return DigitalTwinSnapshot(
        generated_at=datetime.now(UTC),
        orders=(
            make_order(
                order_id=1,
                city="Jilin",
                probability=0.20,
            ),
            make_order(
                order_id=2,
                city="Jilin",
                probability=0.40,
            ),
            make_order(
                order_id=3,
                city="Yantai",
                probability=0.10,
            ),
        ),
    )


def test_neutral_scenario_preserves_probabilities() -> None:
    snapshot = make_snapshot()

    result = SimulationEngine().run(
        snapshot,
        SimulationScenario(
            name="baseline",
        ),
    )

    assert result.total_orders == 3
    assert result.at_risk_delta == 0

    for order in result.orders:
        assert (
            order.simulated_probability
            == pytest.approx(
                order.baseline_probability
            )
        )


def test_demand_increase_raises_risk() -> None:
    snapshot = make_snapshot()

    result = SimulationEngine().run(
        snapshot,
        SimulationScenario(
            name="demand-up",
            demand_multiplier=1.5,
        ),
    )

    assert (
        result.simulated_average_probability
        > result.baseline_average_probability
    )


def test_capacity_reduction_raises_risk() -> None:
    snapshot = make_snapshot()

    result = SimulationEngine().run(
        snapshot,
        SimulationScenario(
            name="capacity-down",
            courier_capacity_multiplier=0.7,
        ),
    )

    assert (
        result.simulated_average_probability
        > result.baseline_average_probability
    )


def test_relaxed_sla_reduces_risk() -> None:
    snapshot = make_snapshot()

    result = SimulationEngine().run(
        snapshot,
        SimulationScenario(
            name="sla-relaxed",
            sla_multiplier=1.5,
        ),
    )

    assert (
        result.simulated_average_probability
        < result.baseline_average_probability
    )


def test_target_city_only_changes_selected_city() -> None:
    snapshot = make_snapshot()

    result = SimulationEngine().run(
        snapshot,
        SimulationScenario(
            name="jilin-stress",
            demand_multiplier=2.0,
            target_city="Jilin",
        ),
    )

    jilin_orders = [
        order
        for order in result.orders
        if order.city == "Jilin"
    ]

    yantai_order = next(
        order
        for order in result.orders
        if order.city == "Yantai"
    )

    assert all(
        order.scenario_applied
        for order in jilin_orders
    )

    assert yantai_order.scenario_applied is False

    assert (
        yantai_order.simulated_probability
        == yantai_order.baseline_probability
    )


def test_simulation_does_not_mutate_snapshot() -> None:
    snapshot = make_snapshot()

    baseline = tuple(
        order.delay_probability
        for order in snapshot.orders
    )

    SimulationEngine().run(
        snapshot,
        SimulationScenario(
            name="stress",
            demand_multiplier=2.0,
        ),
    )

    assert tuple(
        order.delay_probability
        for order in snapshot.orders
    ) == baseline


def test_simulation_is_deterministic() -> None:
    snapshot = make_snapshot()

    scenario = SimulationScenario(
        name="stress",
        demand_multiplier=1.3,
        courier_capacity_multiplier=0.8,
    )

    engine = SimulationEngine()

    first = engine.run(
        snapshot,
        scenario,
    )

    second = engine.run(
        snapshot,
        scenario,
    )

    first_probabilities = tuple(
        order.simulated_probability
        for order in first.orders
    )

    second_probabilities = tuple(
        order.simulated_probability
        for order in second.orders
    )

    assert (
        first_probabilities
        == second_probabilities
    )


def test_newly_at_risk_is_detected() -> None:
    snapshot = DigitalTwinSnapshot(
        generated_at=datetime.now(UTC),
        orders=(
            make_order(
                order_id=1,
                probability=0.24,
            ),
        ),
    )

    result = SimulationEngine().run(
        snapshot,
        SimulationScenario(
            name="stress",
            demand_multiplier=2.0,
        ),
    )

    assert result.baseline_at_risk == 0
    assert result.simulated_at_risk == 1
    assert result.newly_at_risk == 1
    assert result.at_risk_delta == 1


def test_risk_recovery_is_detected() -> None:
    snapshot = DigitalTwinSnapshot(
        generated_at=datetime.now(UTC),
        orders=(
            make_order(
                order_id=1,
                probability=0.30,
            ),
        ),
    )

    result = SimulationEngine().run(
        snapshot,
        SimulationScenario(
            name="relief",
            courier_capacity_multiplier=2.0,
        ),
    )

    assert result.baseline_at_risk == 1
    assert result.simulated_at_risk == 0
    assert result.recovered_from_risk == 1


def test_city_aggregation() -> None:
    result = SimulationEngine().run(
        make_snapshot(),
        SimulationScenario(
            name="jilin-demand",
            demand_multiplier=1.5,
            target_city="Jilin",
        ),
    )

    cities = {
        state.city: state
        for state in result.by_city()
    }

    assert cities["Jilin"].baseline_orders == 2

    assert (
        cities["Jilin"].simulated_demand_units
        == pytest.approx(3.0)
    )

    assert (
        cities["Yantai"].simulated_demand_units
        == pytest.approx(1.0)
    )


@pytest.mark.parametrize(
    (
        "field",
        "value",
    ),
    [
        ("demand_multiplier", 0.0),
        ("courier_capacity_multiplier", 0.0),
        ("sla_multiplier", 0.0),
    ],
)
def test_invalid_multiplier_rejected(
    field: str,
    value: float,
) -> None:
    kwargs = {
        field: value,
    }

    with pytest.raises(
        ValueError,
        match=field,
    ):
        SimulationScenario(
            name="invalid",
            **kwargs,
        )


def test_negative_stress_strength_rejected() -> None:
    with pytest.raises(
        ValueError,
        match="stress_strength",
    ):
        SimulationScenario(
            name="invalid",
            stress_strength=-1.0,
        )


def test_zero_and_one_probabilities_remain_bounded() -> None:
    snapshot = DigitalTwinSnapshot(
        generated_at=datetime.now(UTC),
        orders=(
            make_order(
                order_id=1,
                probability=0.0,
            ),
            make_order(
                order_id=2,
                probability=1.0,
            ),
        ),
    )

    result = SimulationEngine().run(
        snapshot,
        SimulationScenario(
            name="extreme",
            demand_multiplier=10.0,
        ),
    )

    assert result.orders[0].simulated_probability == 0.0
    assert result.orders[1].simulated_probability == 1.0