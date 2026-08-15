from __future__ import annotations

from typing import Annotated, Protocol

from fastapi import Depends, FastAPI

from smartlogix.api.schemas import (
    ExperimentRequest,
    OptimizationRequest,
    SimulationRequest,
)
from smartlogix.digital_twin.experiments import (
    ScenarioExperimentRunner,
)
from smartlogix.digital_twin.models import (
    DigitalTwinSnapshot,
)
from smartlogix.digital_twin.optimization import (
    CapacityOptimizer,
    OptimizationCandidate,
    OptimizationProblem,
)
from smartlogix.digital_twin.repository import (
    DigitalTwinRepository,
)
from smartlogix.digital_twin.simulation import (
    SimulationEngine,
    SimulationScenario,
)
from smartlogix.streaming.postgres_sink import (
    RealtimePostgresConfig,
)


class SnapshotRepository(Protocol):
    def load_snapshot(
        self,
    ) -> DigitalTwinSnapshot:
        ...


def get_repository() -> SnapshotRepository:
    return DigitalTwinRepository(
        RealtimePostgresConfig.from_env()
    )


RepositoryDependency = Annotated[
    SnapshotRepository,
    Depends(get_repository),
]


def _scenario_from_request(
    request: SimulationRequest,
) -> SimulationScenario:
    return SimulationScenario(
        name=request.name,
        demand_multiplier=(
            request.demand_multiplier
        ),
        courier_capacity_multiplier=(
            request.courier_capacity_multiplier
        ),
        sla_multiplier=request.sla_multiplier,
        stress_strength=request.stress_strength,
        target_city=request.target_city,
    )


def _candidate_payload(
    candidate: OptimizationCandidate,
) -> dict[str, object]:
    return {
        "capacity_multiplier": (
            candidate.capacity_multiplier
        ),
        "capacity_increase_rate": (
            candidate.capacity_increase_rate
        ),
        "pressure_factor": (
            candidate.pressure_factor
        ),
        "affected_orders": (
            candidate.affected_orders
        ),
        "intervention_cost": (
            candidate.intervention_cost
        ),
        "feasible": candidate.feasible,
        "target_met": candidate.target_met,
        "simulated_at_risk": (
            candidate.simulated_at_risk
        ),
        "simulated_risk_rate": (
            candidate.simulated_risk_rate
        ),
        "simulated_average_probability": (
            candidate.simulated_average_probability
        ),
        "at_risk_delta": (
            candidate.at_risk_delta
        ),
        "newly_at_risk": (
            candidate.newly_at_risk
        ),
        "recovered_from_risk": (
            candidate.recovered_from_risk
        ),
    }


app = FastAPI(
    title="SmartLogix 360 Digital Twin API",
    description=(
        "Internal API exposing the SmartLogix 360 "
        "Digital Twin, simulation and prescriptive "
        "optimization engines."
    ),
    version="1.0.0",
)


@app.get("/health")
def health() -> dict[str, object]:
    return {
        "status": "ok",
        "service": "smartlogix-digital-twin-api",
        "version": "1.0.0",
    }


@app.get("/api/v1/twin/state")
def get_twin_state(
    repository: RepositoryDependency,
) -> dict[str, object]:
    snapshot = repository.load_snapshot()

    return {
        "generated_at": (
            snapshot.generated_at.isoformat()
        ),
        "summary": {
            "total_orders": (
                snapshot.total_orders
            ),
            "at_risk_orders": (
                snapshot.at_risk_orders
            ),
            "risk_rate": snapshot.risk_rate,
            "active_alerts": (
                snapshot.active_alerts
            ),
            "observed_orders": (
                snapshot.observed_orders
            ),
            "observation_rate": (
                snapshot.observation_rate
            ),
            "evaluated_predictions": (
                snapshot.evaluated_predictions
            ),
            "average_delay_probability": (
                snapshot.average_delay_probability
            ),
            "maximum_delay_probability": (
                snapshot.maximum_delay_probability
            ),
            "full_history_orders": (
                snapshot.full_history_orders
            ),
        },
        "prediction_quality": {
            "true_positives": (
                snapshot.true_positives
            ),
            "true_negatives": (
                snapshot.true_negatives
            ),
            "false_positives": (
                snapshot.false_positives
            ),
            "false_negatives": (
                snapshot.false_negatives
            ),
            "accuracy": (
                snapshot.prediction_accuracy
            ),
            "precision": (
                snapshot.prediction_precision
            ),
            "recall": (
                snapshot.prediction_recall
            ),
        },
        "cities": [
            {
                "city": city.city,
                "total_orders": (
                    city.total_orders
                ),
                "at_risk_orders": (
                    city.at_risk_orders
                ),
                "risk_rate": city.risk_rate,
                "average_delay_probability": (
                    city.average_delay_probability
                ),
                "maximum_delay_probability": (
                    city.maximum_delay_probability
                ),
                "full_history_orders": (
                    city.full_history_orders
                ),
                "full_history_rate": (
                    city.full_history_rate
                ),
                "observed_orders": (
                    city.observed_orders
                ),
                "observation_rate": (
                    city.observation_rate
                ),
                "actual_late_orders": (
                    city.actual_late_orders
                ),
                "actual_late_rate": (
                    city.actual_late_rate
                ),
            }
            for city in snapshot.by_city()
        ],
    }


@app.post("/api/v1/twin/simulations")
def simulate(
    request: SimulationRequest,
    repository: RepositoryDependency,
) -> dict[str, object]:
    snapshot = repository.load_snapshot()

    scenario = _scenario_from_request(
        request
    )

    result = SimulationEngine().run(
        snapshot,
        scenario,
    )

    return {
        "generated_at": (
            result.generated_at.isoformat()
        ),
        "scenario": {
            "name": result.scenario.name,
            "target_city": (
                result.scenario.target_city
            ),
            "demand_multiplier": (
                result.scenario.demand_multiplier
            ),
            "courier_capacity_multiplier": (
                result.scenario
                .courier_capacity_multiplier
            ),
            "sla_multiplier": (
                result.scenario.sla_multiplier
            ),
            "stress_strength": (
                result.scenario.stress_strength
            ),
            "pressure_factor": (
                result.scenario.pressure_factor
            ),
        },
        "summary": {
            "total_orders": (
                result.total_orders
            ),
            "affected_orders": (
                result.affected_orders
            ),
            "baseline_at_risk": (
                result.baseline_at_risk
            ),
            "simulated_at_risk": (
                result.simulated_at_risk
            ),
            "at_risk_delta": (
                result.at_risk_delta
            ),
            "newly_at_risk": (
                result.newly_at_risk
            ),
            "recovered_from_risk": (
                result.recovered_from_risk
            ),
            "baseline_risk_rate": (
                result.baseline_risk_rate
            ),
            "simulated_risk_rate": (
                result.simulated_risk_rate
            ),
            "baseline_average_probability": (
                result.baseline_average_probability
            ),
            "simulated_average_probability": (
                result
                .simulated_average_probability
            ),
            "average_probability_delta": (
                result.average_probability_delta
            ),
        },
        "cities": [
            {
                "city": city.city,
                "baseline_orders": (
                    city.baseline_orders
                ),
                "simulated_demand_units": (
                    city.simulated_demand_units
                ),
                "baseline_at_risk": (
                    city.baseline_at_risk
                ),
                "simulated_at_risk": (
                    city.simulated_at_risk
                ),
                "at_risk_delta": (
                    city.at_risk_delta
                ),
                "baseline_risk_rate": (
                    city.baseline_risk_rate
                ),
                "simulated_risk_rate": (
                    city.simulated_risk_rate
                ),
            }
            for city in result.by_city()
        ],
    }


@app.post("/api/v1/twin/experiments")
def experiment(
    request: ExperimentRequest,
    repository: RepositoryDependency,
) -> dict[str, object]:
    snapshot = repository.load_snapshot()

    scenarios = tuple(
        _scenario_from_request(item)
        for item in request.scenarios
    )

    result = ScenarioExperimentRunner().run(
        snapshot,
        scenarios,
    )

    return {
        "generated_at": (
            result.generated_at.isoformat()
        ),
        "baseline": {
            "total_orders": (
                result.baseline_total_orders
            ),
            "at_risk": (
                result.baseline_at_risk
            ),
            "average_probability": (
                result.baseline_average_probability
            ),
        },
        "best_scenario": (
            result.best_scenario.name
        ),
        "worst_scenario": (
            result.worst_scenario.name
        ),
        "comparisons": [
            {
                "name": item.name,
                "target_city": (
                    item.target_city
                ),
                "pressure_factor": (
                    item.pressure_factor
                ),
                "affected_orders": (
                    item.affected_orders
                ),
                "simulated_demand_units": (
                    item.simulated_demand_units
                ),
                "baseline_at_risk": (
                    item.baseline_at_risk
                ),
                "simulated_at_risk": (
                    item.simulated_at_risk
                ),
                "at_risk_delta": (
                    item.at_risk_delta
                ),
                "baseline_risk_rate": (
                    item.baseline_risk_rate
                ),
                "simulated_risk_rate": (
                    item.simulated_risk_rate
                ),
                "risk_rate_delta": (
                    item.risk_rate_delta
                ),
                "simulated_average_probability": (
                    item
                    .simulated_average_probability
                ),
                "average_probability_delta": (
                    item
                    .average_probability_delta
                ),
                "newly_at_risk": (
                    item.newly_at_risk
                ),
                "recovered_from_risk": (
                    item.recovered_from_risk
                ),
            }
            for item in result.comparisons
        ],
    }


@app.post("/api/v1/twin/optimizations")
def optimize(
    request: OptimizationRequest,
    repository: RepositoryDependency,
) -> dict[str, object]:
    snapshot = repository.load_snapshot()

    problem = OptimizationProblem(
        name=request.name,
        demand_multiplier=(
            request.demand_multiplier
        ),
        capacity_min_multiplier=(
            request.capacity_min_multiplier
        ),
        capacity_max_multiplier=(
            request.capacity_max_multiplier
        ),
        capacity_step=request.capacity_step,
        budget=request.budget,
        capacity_unit_cost=(
            request.capacity_unit_cost
        ),
        max_risk_rate=(
            request.max_risk_rate
        ),
        stress_strength=(
            request.stress_strength
        ),
        target_city=request.target_city,
    )

    result = CapacityOptimizer().optimize(
        snapshot,
        problem,
    )

    return {
        "generated_at": (
            result.generated_at.isoformat()
        ),
        "baseline": {
            "total_orders": (
                result.baseline_total_orders
            ),
            "at_risk": (
                result.baseline_at_risk
            ),
            "risk_rate": (
                result.baseline_risk_rate
            ),
            "average_probability": (
                result.baseline_average_probability
            ),
        },
        "reference": _candidate_payload(
            result.reference_candidate
        ),
        "recommended": _candidate_payload(
            result.recommended
        ),
        "decision": {
            "target_met": result.target_met,
            "risk_reduction": (
                result.risk_reduction
            ),
            "risk_rate_reduction": (
                result.risk_rate_reduction
            ),
            "risk_delta_vs_baseline": (
                result.risk_delta_vs_baseline
            ),
            "risk_rate_delta_vs_baseline": (
                result
                .risk_rate_delta_vs_baseline
            ),
        },
        "candidates": [
            _candidate_payload(candidate)
            for candidate in result.candidates
        ],
    }