from __future__ import annotations

import argparse
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from smartlogix.digital_twin.models import DigitalTwinSnapshot
from smartlogix.digital_twin.simulation import (
    SimulationEngine,
    SimulationResult,
    SimulationScenario,
)


@dataclass(frozen=True, slots=True)
class ScenarioComparison:
    """Resume comparable d'un scenario de simulation."""

    name: str
    target_city: str | None

    pressure_factor: float
    affected_orders: int
    simulated_demand_units: float

    baseline_at_risk: int
    simulated_at_risk: int
    at_risk_delta: int

    baseline_risk_rate: float
    simulated_risk_rate: float

    baseline_average_probability: float
    simulated_average_probability: float
    average_probability_delta: float

    newly_at_risk: int
    recovered_from_risk: int

    @property
    def risk_rate_delta(self) -> float:
        return (
            self.simulated_risk_rate
            - self.baseline_risk_rate
        )

    @classmethod
    def from_result(
        cls,
        result: SimulationResult,
    ) -> ScenarioComparison:
        demand_units = sum(
            city.simulated_demand_units
            for city in result.by_city()
        )

        return cls(
            name=result.scenario.name,
            target_city=result.scenario.target_city,
            pressure_factor=result.scenario.pressure_factor,
            affected_orders=result.affected_orders,
            simulated_demand_units=demand_units,
            baseline_at_risk=result.baseline_at_risk,
            simulated_at_risk=result.simulated_at_risk,
            at_risk_delta=result.at_risk_delta,
            baseline_risk_rate=result.baseline_risk_rate,
            simulated_risk_rate=result.simulated_risk_rate,
            baseline_average_probability=(
                result.baseline_average_probability
            ),
            simulated_average_probability=(
                result.simulated_average_probability
            ),
            average_probability_delta=(
                result.average_probability_delta
            ),
            newly_at_risk=result.newly_at_risk,
            recovered_from_risk=(
                result.recovered_from_risk
            ),
        )


@dataclass(frozen=True, slots=True)
class ScenarioExperimentResult:
    """Resultat d'une campagne de scenarios."""

    generated_at: datetime

    baseline_total_orders: int
    baseline_at_risk: int
    baseline_average_probability: float

    comparisons: tuple[ScenarioComparison, ...]

    def __post_init__(self) -> None:
        if self.generated_at.tzinfo is None:
            raise ValueError(
                "generated_at doit etre timezone-aware."
            )

    def sorted_by_risk(
        self,
        *,
        descending: bool = False,
    ) -> tuple[ScenarioComparison, ...]:
        return tuple(
            sorted(
                self.comparisons,
                key=lambda item: (
                    item.simulated_at_risk,
                    item.simulated_average_probability,
                    item.name,
                ),
                reverse=descending,
            )
        )

    @property
    def best_scenario(
        self,
    ) -> ScenarioComparison:
        if not self.comparisons:
            raise ValueError(
                "Aucun scenario disponible."
            )

        return min(
            self.comparisons,
            key=lambda item: (
                item.simulated_at_risk,
                item.simulated_average_probability,
                item.pressure_factor,
                item.name,
            ),
        )

    @property
    def worst_scenario(
        self,
    ) -> ScenarioComparison:
        if not self.comparisons:
            raise ValueError(
                "Aucun scenario disponible."
            )

        return max(
            self.comparisons,
            key=lambda item: (
                item.simulated_at_risk,
                item.simulated_average_probability,
                item.pressure_factor,
                item.name,
            ),
        )

    def to_text_table(self) -> str:
        header = (
            f"{'SCENARIO':<34}"
            f"{'TARGET':<12}"
            f"{'PRESS':>8}"
            f"{'RISK':>8}"
            f"{'DRISK':>8}"
            f"{'RISK%':>9}"
            f"{'AVG_P':>10}"
            f"{'DAVG':>10}"
            f"{'NEW':>7}"
            f"{'RECOV':>8}"
        )

        separator = "-" * len(header)

        rows = [
            header,
            separator,
        ]

        for item in self.comparisons:
            target = item.target_city or "ALL"

            rows.append(
                f"{item.name:<34}"
                f"{target:<12}"
                f"{item.pressure_factor:>8.3f}"
                f"{item.simulated_at_risk:>8}"
                f"{item.at_risk_delta:>+8}"
                f"{item.simulated_risk_rate:>9.3f}"
                f"{item.simulated_average_probability:>10.4f}"
                f"{item.average_probability_delta:>+10.4f}"
                f"{item.newly_at_risk:>7}"
                f"{item.recovered_from_risk:>8}"
            )

        return "\n".join(rows)


@dataclass(frozen=True, slots=True)
class ScenarioExperimentRunner:
    """Execute une campagne reproductible de simulations."""

    engine: SimulationEngine = SimulationEngine()

    def run(
        self,
        snapshot: DigitalTwinSnapshot,
        scenarios: Sequence[SimulationScenario],
        *,
        generated_at: datetime | None = None,
    ) -> ScenarioExperimentResult:
        if not scenarios:
            raise ValueError(
                "La campagne doit contenir au moins un scenario."
            )

        names = [
            scenario.name
            for scenario in scenarios
        ]

        if len(names) != len(set(names)):
            raise ValueError(
                "Les noms des scenarios doivent etre uniques."
            )

        timestamp = generated_at or datetime.now(UTC)

        comparisons = tuple(
            ScenarioComparison.from_result(
                self.engine.run(
                    snapshot,
                    scenario,
                    generated_at=timestamp,
                )
            )
            for scenario in scenarios
        )

        return ScenarioExperimentResult(
            generated_at=timestamp,
            baseline_total_orders=snapshot.total_orders,
            baseline_at_risk=snapshot.at_risk_orders,
            baseline_average_probability=(
                snapshot.average_delay_probability
            ),
            comparisons=comparisons,
        )


def build_default_scenarios(
    *,
    target_city: str | None = None,
) -> tuple[SimulationScenario, ...]:
    """
    Catalogue standard des scenarios experimentaux V2.2.
    """

    scenarios: list[SimulationScenario] = [
        SimulationScenario(
            name="baseline",
        ),
        SimulationScenario(
            name="demand+10",
            demand_multiplier=1.10,
        ),
        SimulationScenario(
            name="demand+20",
            demand_multiplier=1.20,
        ),
        SimulationScenario(
            name="demand+50",
            demand_multiplier=1.50,
        ),
        SimulationScenario(
            name="capacity-10",
            courier_capacity_multiplier=0.90,
        ),
        SimulationScenario(
            name="capacity-20",
            courier_capacity_multiplier=0.80,
        ),
        SimulationScenario(
            name="capacity-30",
            courier_capacity_multiplier=0.70,
        ),
        SimulationScenario(
            name="demand+20_capacity-20",
            demand_multiplier=1.20,
            courier_capacity_multiplier=0.80,
        ),
        SimulationScenario(
            name="demand+20_capacity-20_sla+10",
            demand_multiplier=1.20,
            courier_capacity_multiplier=0.80,
            sla_multiplier=1.10,
        ),
        SimulationScenario(
            name="capacity+20",
            courier_capacity_multiplier=1.20,
        ),
        SimulationScenario(
            name="sla+20",
            sla_multiplier=1.20,
        ),
    ]

    if target_city is not None:
        scenarios.append(
            SimulationScenario(
                name=f"{target_city.lower()}_local_stress",
                demand_multiplier=1.30,
                courier_capacity_multiplier=0.80,
                target_city=target_city,
            )
        )

    return tuple(scenarios)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Execute la campagne experimentale "
            "du Digital Twin SmartLogix."
        )
    )

    parser.add_argument(
        "--target-city",
        default=None,
        help=(
            "Ajoute un scenario de stress local "
            "sur la ville indiquee."
        ),
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

    scenarios = build_default_scenarios(
        target_city=args.target_city
    )

    experiment = ScenarioExperimentRunner().run(
        snapshot,
        scenarios,
    )

    print("DIGITAL_TWIN_EXPERIMENT_V2_2")
    print(
        "baseline_orders=",
        experiment.baseline_total_orders,
    )
    print(
        "baseline_at_risk=",
        experiment.baseline_at_risk,
    )
    print(
        "baseline_avg_probability=",
        round(
            experiment.baseline_average_probability,
            4,
        ),
    )

    print()
    print(experiment.to_text_table())

    print()
    print(
        "BEST=",
        experiment.best_scenario.name,
        "risk=",
        experiment.best_scenario.simulated_at_risk,
    )

    print(
        "WORST=",
        experiment.worst_scenario.name,
        "risk=",
        experiment.worst_scenario.simulated_at_risk,
    )


if __name__ == "__main__":
    main()