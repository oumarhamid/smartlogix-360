from datetime import UTC, datetime

import pytest

from smartlogix.digital_twin.models import (
    DigitalTwinSnapshot,
    TwinOrderState,
)
from smartlogix.digital_twin.optimization import (
    CapacityOptimizer,
    OptimizationProblem,
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
        predicted_late=probability >= threshold,
        threshold=threshold,
        model_name="lightgbm",
        model_version="lightgbm-delay-v1",
        courier_prev_day_available=True,
        city_prev_day_available=True,
        alert_active=probability >= threshold,
        updated_at=timestamp,
    )


def make_snapshot() -> DigitalTwinSnapshot:
    return DigitalTwinSnapshot(
        generated_at=datetime.now(UTC),
        orders=(
            make_order(order_id=1, probability=0.20),
            make_order(order_id=2, probability=0.24),
            make_order(order_id=3, probability=0.40),
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


def test_optimizer_generates_capacity_grid() -> None:
    result = CapacityOptimizer().optimize(
        make_snapshot(),
        OptimizationProblem(
            name="grid",
            capacity_min_multiplier=1.0,
            capacity_max_multiplier=1.2,
            capacity_step=0.1,
        ),
    )

    assert tuple(
        candidate.capacity_multiplier
        for candidate in result.candidates
    ) == (1.0, 1.1, 1.2)


def test_grid_includes_non_aligned_maximum() -> None:
    result = CapacityOptimizer().optimize(
        make_snapshot(),
        OptimizationProblem(
            name="grid-max",
            capacity_min_multiplier=1.0,
            capacity_max_multiplier=1.23,
            capacity_step=0.1,
        ),
    )

    assert result.candidates[-1].capacity_multiplier == 1.23


def test_capacity_reduces_stress_risk() -> None:
    result = CapacityOptimizer().optimize(
        make_snapshot(),
        OptimizationProblem(
            name="stress",
            demand_multiplier=1.5,
            capacity_max_multiplier=1.5,
            capacity_step=0.5,
            budget=1.0,
        ),
    )

    first = result.candidates[0]
    last = result.candidates[-1]

    assert (
        last.simulated_average_probability
        < first.simulated_average_probability
    )
    assert last.simulated_at_risk <= first.simulated_at_risk


def test_budget_marks_expensive_candidates_infeasible() -> None:
    result = CapacityOptimizer().optimize(
        make_snapshot(),
        OptimizationProblem(
            name="budget",
            capacity_max_multiplier=1.5,
            capacity_step=0.25,
            budget=0.20,
        ),
    )

    candidate = next(
        item
        for item in result.candidates
        if item.capacity_multiplier == 1.25
    )

    assert candidate.feasible is False


def test_no_intervention_is_zero_cost() -> None:
    result = CapacityOptimizer().optimize(
        make_snapshot(),
        OptimizationProblem(
            name="baseline",
            capacity_max_multiplier=1.0,
            budget=0.0,
        ),
    )

    candidate = result.candidates[0]

    assert candidate.intervention_cost == 0.0
    assert candidate.feasible is True


def test_optimizer_selects_minimum_cost_target_solution() -> None:
    snapshot = make_snapshot()
    baseline_rate = snapshot.risk_rate

    result = CapacityOptimizer().optimize(
        snapshot,
        OptimizationProblem(
            name="restore-baseline",
            demand_multiplier=1.2,
            capacity_max_multiplier=1.5,
            capacity_step=0.1,
            budget=0.5,
            max_risk_rate=baseline_rate,
        ),
    )

    recommended = result.recommended

    assert result.target_met is True
    assert recommended.simulated_risk_rate <= baseline_rate

    cheaper = [
        candidate
        for candidate in result.feasible_candidates
        if (
            candidate.intervention_cost
            < recommended.intervention_cost
        )
    ]

    assert all(
        candidate.simulated_risk_rate > baseline_rate
        for candidate in cheaper
    )


def test_optimizer_returns_best_feasible_when_target_impossible() -> None:
    result = CapacityOptimizer().optimize(
        make_snapshot(),
        OptimizationProblem(
            name="constrained",
            demand_multiplier=2.0,
            capacity_max_multiplier=1.5,
            capacity_step=0.1,
            budget=0.1,
            max_risk_rate=0.0,
        ),
    )

    assert result.target_met is False

    assert result.recommended.simulated_at_risk == min(
        candidate.simulated_at_risk
        for candidate in result.feasible_candidates
    )


def test_target_city_reduces_intervention_cost() -> None:
    snapshot = make_snapshot()

    global_result = CapacityOptimizer().optimize(
        snapshot,
        OptimizationProblem(
            name="global",
            capacity_max_multiplier=1.2,
            capacity_step=0.2,
            budget=1.0,
        ),
    )

    local_result = CapacityOptimizer().optimize(
        snapshot,
        OptimizationProblem(
            name="local",
            capacity_max_multiplier=1.2,
            capacity_step=0.2,
            budget=1.0,
            target_city="Yantai",
        ),
    )

    global_candidate = global_result.candidates[-1]
    local_candidate = local_result.candidates[-1]

    assert (
        local_candidate.intervention_cost
        < global_candidate.intervention_cost
    )
    assert local_candidate.affected_orders == 2


def test_risk_reduction_uses_stressed_reference() -> None:
    result = CapacityOptimizer().optimize(
        make_snapshot(),
        OptimizationProblem(
            name="stressed",
            demand_multiplier=1.5,
            capacity_max_multiplier=1.5,
            capacity_step=0.05,
            budget=0.25,
        ),
    )

    assert result.risk_reduction == (
        result.reference_candidate.simulated_at_risk
        - result.recommended.simulated_at_risk
    )
    assert result.risk_reduction >= 0


def test_risk_delta_vs_baseline_keeps_original_context() -> None:
    result = CapacityOptimizer().optimize(
        make_snapshot(),
        OptimizationProblem(
            name="stressed",
            demand_multiplier=1.5,
            capacity_max_multiplier=1.5,
            capacity_step=0.05,
            budget=0.25,
        ),
    )

    assert result.risk_delta_vs_baseline == (
        result.recommended.simulated_at_risk
        - result.baseline_at_risk
    )


def test_reference_candidate_is_minimum_capacity() -> None:
    result = CapacityOptimizer().optimize(
        make_snapshot(),
        OptimizationProblem(
            name="reference",
            capacity_min_multiplier=1.1,
            capacity_max_multiplier=1.3,
            capacity_step=0.1,
            budget=1.0,
        ),
    )

    assert result.reference_candidate.capacity_multiplier == 1.1


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("demand_multiplier", 0.0),
        ("capacity_min_multiplier", 0.9),
        ("capacity_step", 0.0),
        ("budget", -1.0),
        ("capacity_unit_cost", 0.0),
    ],
)
def test_invalid_problem_values_rejected(
    field: str,
    value: float,
) -> None:
    with pytest.raises(ValueError):
        OptimizationProblem(
            name="invalid",
            **{field: value},
        )


def test_invalid_risk_target_rejected() -> None:
    with pytest.raises(
        ValueError,
        match="max_risk_rate",
    ):
        OptimizationProblem(
            name="invalid",
            max_risk_rate=1.5,
        )


def test_result_table_contains_decision_columns() -> None:
    result = CapacityOptimizer().optimize(
        make_snapshot(),
        OptimizationProblem(
            name="table",
            capacity_max_multiplier=1.1,
            capacity_step=0.1,
        ),
    )

    table = result.to_text_table()

    assert "CAPACITY" in table
    assert "COST" in table
    assert "TARGET" in table
    assert "RISK" in table
    assert "RECOV" in table


def test_optimization_uses_requested_timestamp() -> None:
    timestamp = datetime(
        2026,
        8,
        15,
        21,
        30,
        tzinfo=UTC,
    )

    result = CapacityOptimizer().optimize(
        make_snapshot(),
        OptimizationProblem(
            name="timestamp",
            capacity_max_multiplier=1.0,
        ),
        generated_at=timestamp,
    )

    assert result.generated_at == timestamp