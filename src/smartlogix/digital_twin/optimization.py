from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from math import isclose

from smartlogix.digital_twin.models import (
    DigitalTwinSnapshot,
)
from smartlogix.digital_twin.simulation import (
    SimulationEngine,
    SimulationScenario,
)


@dataclass(frozen=True, slots=True)
class OptimizationProblem:
    """Probleme d'optimisation prescriptive du Digital Twin."""

    name: str

    demand_multiplier: float = 1.0

    capacity_min_multiplier: float = 1.0
    capacity_max_multiplier: float = 1.5
    capacity_step: float = 0.05

    budget: float = 0.50
    capacity_unit_cost: float = 1.0

    max_risk_rate: float | None = None

    stress_strength: float = 1.0
    target_city: str | None = None

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError(
                "Le nom du probleme ne peut pas etre vide."
            )

        if self.demand_multiplier <= 0:
            raise ValueError(
                "demand_multiplier doit etre "
                "strictement positif."
            )

        if self.capacity_min_multiplier < 1.0:
            raise ValueError(
                "capacity_min_multiplier doit etre "
                "superieur ou egal a 1."
            )

        if (
            self.capacity_max_multiplier
            < self.capacity_min_multiplier
        ):
            raise ValueError(
                "capacity_max_multiplier doit etre "
                "superieur ou egal a capacity_min_multiplier."
            )

        if self.capacity_step <= 0:
            raise ValueError(
                "capacity_step doit etre strictement positif."
            )

        if self.budget < 0:
            raise ValueError(
                "budget doit etre positif ou nul."
            )

        if self.capacity_unit_cost <= 0:
            raise ValueError(
                "capacity_unit_cost doit etre "
                "strictement positif."
            )

        if (
            self.max_risk_rate is not None
            and not 0.0 <= self.max_risk_rate <= 1.0
        ):
            raise ValueError(
                "max_risk_rate doit etre compris entre 0 et 1."
            )

        if self.stress_strength < 0:
            raise ValueError(
                "stress_strength doit etre positif ou nul."
            )

        if (
            self.target_city is not None
            and not self.target_city.strip()
        ):
            raise ValueError(
                "target_city ne peut pas etre vide."
            )


@dataclass(frozen=True, slots=True)
class OptimizationCandidate:
    """Solution candidate evaluee par simulation."""

    capacity_multiplier: float

    pressure_factor: float
    affected_orders: int

    intervention_cost: float
    feasible: bool
    target_met: bool

    simulated_at_risk: int
    simulated_risk_rate: float

    simulated_average_probability: float

    at_risk_delta: int
    newly_at_risk: int
    recovered_from_risk: int

    @property
    def capacity_increase_rate(self) -> float:
        return self.capacity_multiplier - 1.0


@dataclass(frozen=True, slots=True)
class OptimizationResult:
    """Resultat complet de la recherche prescriptive."""

    problem: OptimizationProblem

    generated_at: datetime

    baseline_total_orders: int
    baseline_at_risk: int
    baseline_risk_rate: float
    baseline_average_probability: float

    candidates: tuple[OptimizationCandidate, ...]

    def __post_init__(self) -> None:
        if self.generated_at.tzinfo is None:
            raise ValueError(
                "generated_at doit etre timezone-aware."
            )

        if not self.candidates:
            raise ValueError(
                "L'optimisation doit contenir "
                "au moins un candidat."
            )

    @property
    def feasible_candidates(
        self,
    ) -> tuple[OptimizationCandidate, ...]:
        return tuple(
            candidate
            for candidate in self.candidates
            if candidate.feasible
        )

    @property
    def target_candidates(
        self,
    ) -> tuple[OptimizationCandidate, ...]:
        return tuple(
            candidate
            for candidate in self.feasible_candidates
            if candidate.target_met
        )

    @property
    def target_met(self) -> bool:
        if self.problem.max_risk_rate is None:
            return True

        return bool(self.target_candidates)

    @property
    def recommended(
        self,
    ) -> OptimizationCandidate:
        feasible = self.feasible_candidates

        if not feasible:
            raise ValueError(
                "Aucun candidat ne respecte le budget."
            )

        if (
            self.problem.max_risk_rate is not None
            and self.target_candidates
        ):
            return min(
                self.target_candidates,
                key=lambda candidate: (
                    candidate.intervention_cost,
                    candidate.simulated_at_risk,
                    candidate.simulated_average_probability,
                    candidate.capacity_multiplier,
                ),
            )

        return min(
            feasible,
            key=lambda candidate: (
                candidate.simulated_at_risk,
                candidate.simulated_average_probability,
                candidate.intervention_cost,
                candidate.capacity_multiplier,
            ),
        )

    @property
    def reference_candidate(self) -> OptimizationCandidate:
        return min(
            self.candidates,
            key=lambda candidate: candidate.capacity_multiplier,
        )

    @property
    def risk_reduction(self) -> int:
        return (
            self.reference_candidate.simulated_at_risk
            - self.recommended.simulated_at_risk
        )

    @property
    def risk_rate_reduction(self) -> float:
        return (
            self.reference_candidate.simulated_risk_rate
            - self.recommended.simulated_risk_rate
        )

    @property
    def risk_delta_vs_baseline(self) -> int:
        return self.recommended.simulated_at_risk - self.baseline_at_risk

    @property
    def risk_rate_delta_vs_baseline(self) -> float:
        return self.recommended.simulated_risk_rate - self.baseline_risk_rate

    def to_text_table(self) -> str:
        header = (
            f"{'CAPACITY':>10}"
            f"{'PRESS':>9}"
            f"{'COST':>9}"
            f"{'FEAS':>7}"
            f"{'TARGET':>8}"
            f"{'RISK':>8}"
            f"{'RISK%':>9}"
            f"{'DRISK':>8}"
            f"{'AVG_P':>10}"
            f"{'NEW':>7}"
            f"{'RECOV':>8}"
        )

        separator = "-" * len(header)

        rows = [
            header,
            separator,
        ]

        for candidate in self.candidates:
            rows.append(
                f"{candidate.capacity_multiplier:>10.2f}"
                f"{candidate.pressure_factor:>9.3f}"
                f"{candidate.intervention_cost:>9.3f}"
                f"{str(candidate.feasible):>7}"
                f"{str(candidate.target_met):>8}"
                f"{candidate.simulated_at_risk:>8}"
                f"{candidate.simulated_risk_rate:>9.3f}"
                f"{candidate.at_risk_delta:>+8}"
                f"{candidate.simulated_average_probability:>10.4f}"
                f"{candidate.newly_at_risk:>7}"
                f"{candidate.recovered_from_risk:>8}"
            )

        return "\n".join(rows)


def _capacity_values(
    problem: OptimizationProblem,
) -> tuple[float, ...]:
    current = Decimal(
        str(problem.capacity_min_multiplier)
    )

    maximum = Decimal(
        str(problem.capacity_max_multiplier)
    )

    step = Decimal(
        str(problem.capacity_step)
    )

    values: list[float] = []

    while current <= maximum:
        values.append(float(current))
        current += step

    if (
        values
        and not isclose(
            values[-1],
            problem.capacity_max_multiplier,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
        and values[-1]
        < problem.capacity_max_multiplier
    ):
        values.append(
            problem.capacity_max_multiplier
        )

    return tuple(values)


@dataclass(frozen=True, slots=True)
class CapacityOptimizer:
    """Recherche deterministe de capacite sous contrainte."""

    engine: SimulationEngine = SimulationEngine()

    def optimize(
        self,
        snapshot: DigitalTwinSnapshot,
        problem: OptimizationProblem,
        *,
        generated_at: datetime | None = None,
    ) -> OptimizationResult:
        timestamp = generated_at or datetime.now(UTC)

        candidates: list[OptimizationCandidate] = []

        for capacity in _capacity_values(problem):
            scenario = SimulationScenario(
                name=(
                    f"{problem.name}_capacity_"
                    f"{capacity:.2f}"
                ),
                demand_multiplier=(
                    problem.demand_multiplier
                ),
                courier_capacity_multiplier=capacity,
                stress_strength=problem.stress_strength,
                target_city=problem.target_city,
            )

            simulation = self.engine.run(
                snapshot,
                scenario,
                generated_at=timestamp,
            )

            affected_fraction = (
                simulation.affected_orders
                / snapshot.total_orders
                if snapshot.total_orders
                else 0.0
            )

            intervention_cost = (
                max(
                    capacity - 1.0,
                    0.0,
                )
                * problem.capacity_unit_cost
                * affected_fraction
            )

            feasible = (
                intervention_cost
                <= problem.budget + 1e-12
            )

            target_met = (
                problem.max_risk_rate is None
                or simulation.simulated_risk_rate
                <= problem.max_risk_rate + 1e-12
            )

            candidates.append(
                OptimizationCandidate(
                    capacity_multiplier=capacity,
                    pressure_factor=(
                        scenario.pressure_factor
                    ),
                    affected_orders=(
                        simulation.affected_orders
                    ),
                    intervention_cost=(
                        intervention_cost
                    ),
                    feasible=feasible,
                    target_met=target_met,
                    simulated_at_risk=(
                        simulation.simulated_at_risk
                    ),
                    simulated_risk_rate=(
                        simulation.simulated_risk_rate
                    ),
                    simulated_average_probability=(
                        simulation
                        .simulated_average_probability
                    ),
                    at_risk_delta=(
                        simulation.at_risk_delta
                    ),
                    newly_at_risk=(
                        simulation.newly_at_risk
                    ),
                    recovered_from_risk=(
                        simulation.recovered_from_risk
                    ),
                )
            )

        return OptimizationResult(
            problem=problem,
            generated_at=timestamp,
            baseline_total_orders=(
                snapshot.total_orders
            ),
            baseline_at_risk=(
                snapshot.at_risk_orders
            ),
            baseline_risk_rate=(
                snapshot.risk_rate
            ),
            baseline_average_probability=(
                snapshot.average_delay_probability
            ),
            candidates=tuple(candidates),
        )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Optimisation prescriptive "
            "du Digital Twin SmartLogix."
        )
    )

    parser.add_argument(
        "--name",
        default="capacity-optimization",
    )

    parser.add_argument(
        "--demand-multiplier",
        type=float,
        default=1.20,
    )

    parser.add_argument(
        "--capacity-min",
        type=float,
        default=1.0,
    )

    parser.add_argument(
        "--capacity-max",
        type=float,
        default=1.5,
    )

    parser.add_argument(
        "--capacity-step",
        type=float,
        default=0.05,
    )

    parser.add_argument(
        "--budget",
        type=float,
        default=0.25,
    )

    parser.add_argument(
        "--max-risk-rate",
        type=float,
        default=None,
    )

    parser.add_argument(
        "--target-city",
        default=None,
    )

    return parser


def main() -> None:
    from smartlogix.digital_twin.repository import (
        DigitalTwinRepository,
    )
    from smartlogix.streaming.postgres_sink import (
        RealtimePostgresConfig,
    )

    args = _build_parser().parse_args()

    snapshot = DigitalTwinRepository(
        RealtimePostgresConfig.from_env()
    ).load_snapshot()

    problem = OptimizationProblem(
        name=args.name,
        demand_multiplier=args.demand_multiplier,
        capacity_min_multiplier=args.capacity_min,
        capacity_max_multiplier=args.capacity_max,
        capacity_step=args.capacity_step,
        budget=args.budget,
        max_risk_rate=args.max_risk_rate,
        target_city=args.target_city,
    )

    result = CapacityOptimizer().optimize(
        snapshot,
        problem,
    )

    recommended = result.recommended

    print("DIGITAL_TWIN_OPTIMIZATION_V3_1")

    print(
        "baseline_orders=",
        result.baseline_total_orders,
    )

    print(
        "baseline_at_risk=",
        result.baseline_at_risk,
    )

    print(
        "baseline_risk_rate=",
        round(
            result.baseline_risk_rate,
            4,
        ),
    )

    print(
        "budget=",
        problem.budget,
    )

    print(
        "target_risk_rate=",
        problem.max_risk_rate,
    )

    print()

    print(result.to_text_table())

    print()

    print(
        "RECOMMENDED_CAPACITY=",
        round(
            recommended.capacity_multiplier,
            4,
        ),
    )

    print(
        "CAPACITY_INCREASE=",
        round(
            recommended.capacity_increase_rate,
            4,
        ),
    )

    print(
        "ESTIMATED_COST=",
        round(
            recommended.intervention_cost,
            4,
        ),
    )

    print(
        "SIMULATED_RISK=",
        recommended.simulated_at_risk,
    )

    print(
        "SIMULATED_RISK_RATE=",
        round(
            recommended.simulated_risk_rate,
            4,
        ),
    )

    print(
        "RISK_REDUCTION=",
        result.risk_reduction,
    )

    print("REFERENCE_RISK=", result.reference_candidate.simulated_at_risk)
    print("RISK_DELTA_VS_BASELINE=", result.risk_delta_vs_baseline)
    print(
        "TARGET_MET=",
        result.target_met,
    )


if __name__ == "__main__":
    main()