from datetime import UTC, datetime

import pytest

from smartlogix.digital_twin.experiments import (
    ScenarioExperimentRunner,
    build_default_scenarios,
)
from smartlogix.digital_twin.models import (
    DigitalTwinSnapshot,
    TwinOrderState,
)
from smartlogix.digital_twin.simulation import (
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
                probability=0.24,
            ),
            make_order(
                order_id=3,
                city="Jilin",
                probability=0.40,
            ),
            make_order(
                order_id=4,
                city="Yantai",
                probability=0.10,
            ),
            make_order(
                order_id=5,
                city="Yantai",
                probability=0.30,
            ),
        ),
    )


def comparison_by_name(
    experiment,
    name: str,
):
    return next(
        item
        for item in experiment.comparisons
        if item.name == name
    )


def test_default_scenario_catalog_is_unique() -> None:
    scenarios = build_default_scenarios(
        target_city="Jilin"
    )

    names = [
        scenario.name
        for scenario in scenarios
    ]

    assert len(names) == len(set(names))


def test_default_catalog_contains_core_cases() -> None:
    names = {
        scenario.name
        for scenario in build_default_scenarios()
    }

    assert "baseline" in names
    assert "demand+20" in names
    assert "capacity-20" in names
    assert "sla+20" in names
    assert "demand+20_capacity-20" in names


def test_runner_rejects_empty_catalog() -> None:
    with pytest.raises(
        ValueError,
        match="au moins un scenario",
    ):
        ScenarioExperimentRunner().run(
            make_snapshot(),
            (),
        )


def test_runner_rejects_duplicate_names() -> None:
    scenarios = (
        SimulationScenario(
            name="duplicate",
        ),
        SimulationScenario(
            name="duplicate",
            demand_multiplier=1.2,
        ),
    )

    with pytest.raises(
        ValueError,
        match="uniques",
    ):
        ScenarioExperimentRunner().run(
            make_snapshot(),
            scenarios,
        )


def test_baseline_comparison_preserves_state() -> None:
    experiment = ScenarioExperimentRunner().run(
        make_snapshot(),
        (
            SimulationScenario(
                name="baseline"
            ),
        ),
    )

    baseline = experiment.comparisons[0]

    assert baseline.at_risk_delta == 0
    assert baseline.average_probability_delta == 0.0
    assert baseline.newly_at_risk == 0
    assert baseline.recovered_from_risk == 0


def test_combined_stress_is_worse_than_demand_only() -> None:
    experiment = ScenarioExperimentRunner().run(
        make_snapshot(),
        (
            SimulationScenario(
                name="demand",
                demand_multiplier=1.2,
            ),
            SimulationScenario(
                name="combined",
                demand_multiplier=1.2,
                courier_capacity_multiplier=0.8,
            ),
        ),
    )

    demand = comparison_by_name(
        experiment,
        "demand",
    )

    combined = comparison_by_name(
        experiment,
        "combined",
    )

    assert (
        combined.simulated_average_probability
        > demand.simulated_average_probability
    )

    assert (
        combined.simulated_at_risk
        >= demand.simulated_at_risk
    )


def test_capacity_relief_reduces_probability() -> None:
    experiment = ScenarioExperimentRunner().run(
        make_snapshot(),
        (
            SimulationScenario(
                name="capacity-relief",
                courier_capacity_multiplier=1.5,
            ),
        ),
    )

    result = experiment.comparisons[0]

    assert result.average_probability_delta < 0
    assert result.at_risk_delta <= 0


def test_target_city_only_affects_target_orders() -> None:
    experiment = ScenarioExperimentRunner().run(
        make_snapshot(),
        (
            SimulationScenario(
                name="local-stress",
                target_city="Jilin",
                demand_multiplier=1.5,
            ),
        ),
    )

    result = experiment.comparisons[0]

    assert result.affected_orders == 3


def test_sorted_by_risk_orders_results() -> None:
    experiment = ScenarioExperimentRunner().run(
        make_snapshot(),
        (
            SimulationScenario(
                name="stress",
                demand_multiplier=2.0,
            ),
            SimulationScenario(
                name="baseline",
            ),
            SimulationScenario(
                name="relief",
                courier_capacity_multiplier=2.0,
            ),
        ),
    )

    ordered = experiment.sorted_by_risk()

    risks = [
        item.simulated_at_risk
        for item in ordered
    ]

    assert risks == sorted(risks)


def test_best_and_worst_scenarios() -> None:
    experiment = ScenarioExperimentRunner().run(
        make_snapshot(),
        (
            SimulationScenario(
                name="stress",
                demand_multiplier=2.0,
            ),
            SimulationScenario(
                name="relief",
                courier_capacity_multiplier=2.0,
            ),
        ),
    )

    assert experiment.best_scenario.name == "relief"
    assert experiment.worst_scenario.name == "stress"


def test_text_table_contains_expected_columns() -> None:
    experiment = ScenarioExperimentRunner().run(
        make_snapshot(),
        (
            SimulationScenario(
                name="baseline",
            ),
        ),
    )

    table = experiment.to_text_table()

    assert "SCENARIO" in table
    assert "PRESS" in table
    assert "RISK" in table
    assert "AVG_P" in table
    assert "RECOV" in table
    assert "baseline" in table


def test_experiment_uses_requested_timestamp() -> None:
    timestamp = datetime(
        2026,
        8,
        15,
        21,
        30,
        tzinfo=UTC,
    )

    experiment = ScenarioExperimentRunner().run(
        make_snapshot(),
        (
            SimulationScenario(
                name="baseline",
            ),
        ),
        generated_at=timestamp,
    )

    assert experiment.generated_at == timestamp