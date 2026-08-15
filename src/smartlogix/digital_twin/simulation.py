from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from math import exp, log, log1p
from statistics import fmean

from smartlogix.digital_twin.models import (
    DigitalTwinSnapshot,
    TwinOrderState,
)


@dataclass(frozen=True, slots=True)
class SimulationScenario:
    """Parametres d'un scenario what-if du jumeau numerique."""

    name: str

    demand_multiplier: float = 1.0
    courier_capacity_multiplier: float = 1.0
    sla_multiplier: float = 1.0

    stress_strength: float = 1.0

    target_city: str | None = None

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError(
                "Le nom du scenario ne peut pas etre vide."
            )

        for name, value in (
            (
                "demand_multiplier",
                self.demand_multiplier,
            ),
            (
                "courier_capacity_multiplier",
                self.courier_capacity_multiplier,
            ),
            (
                "sla_multiplier",
                self.sla_multiplier,
            ),
        ):
            if value <= 0:
                raise ValueError(
                    f"{name} doit etre strictement positif."
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

    @property
    def pressure_factor(self) -> float:
        """
        Pression operationnelle relative.

        > 1 augmente le risque.
        < 1 reduit le risque.
        = 1 conserve le risque de base.
        """

        return (
            self.demand_multiplier
            / self.courier_capacity_multiplier
            / self.sla_multiplier
        )

    def applies_to_city(
        self,
        city: str,
    ) -> bool:
        if self.target_city is None:
            return True

        return city == self.target_city


@dataclass(frozen=True, slots=True)
class SimulatedOrderState:
    """Etat predictif simule d'une commande existante."""

    order_id: int
    city: str
    model_version: str

    threshold: float

    baseline_probability: float
    simulated_probability: float

    baseline_predicted_late: bool
    simulated_predicted_late: bool

    scenario_applied: bool

    @property
    def probability_delta(self) -> float:
        return (
            self.simulated_probability
            - self.baseline_probability
        )

    @property
    def became_at_risk(self) -> bool:
        return (
            not self.baseline_predicted_late
            and self.simulated_predicted_late
        )

    @property
    def recovered_from_risk(self) -> bool:
        return (
            self.baseline_predicted_late
            and not self.simulated_predicted_late
        )


@dataclass(frozen=True, slots=True)
class CitySimulationState:
    """Resultat agrege d'un scenario pour une ville."""

    city: str

    baseline_orders: int
    simulated_demand_units: float

    baseline_at_risk: int
    simulated_at_risk: int

    baseline_average_probability: float
    simulated_average_probability: float

    @property
    def baseline_risk_rate(self) -> float:
        if self.baseline_orders == 0:
            return 0.0

        return (
            self.baseline_at_risk
            / self.baseline_orders
        )

    @property
    def simulated_risk_rate(self) -> float:
        if self.baseline_orders == 0:
            return 0.0

        return (
            self.simulated_at_risk
            / self.baseline_orders
        )

    @property
    def at_risk_delta(self) -> int:
        return (
            self.simulated_at_risk
            - self.baseline_at_risk
        )

    @property
    def average_probability_delta(self) -> float:
        return (
            self.simulated_average_probability
            - self.baseline_average_probability
        )


@dataclass(frozen=True, slots=True)
class SimulationResult:
    """Resultat immutable d'une simulation."""

    scenario: SimulationScenario

    generated_at: datetime
    baseline_snapshot_generated_at: datetime

    orders: tuple[SimulatedOrderState, ...]

    def __post_init__(self) -> None:
        if self.generated_at.tzinfo is None:
            raise ValueError(
                "generated_at doit etre timezone-aware."
            )

        if (
            self.baseline_snapshot_generated_at.tzinfo
            is None
        ):
            raise ValueError(
                "baseline_snapshot_generated_at "
                "doit etre timezone-aware."
            )

    @property
    def total_orders(self) -> int:
        return len(self.orders)

    @property
    def affected_orders(self) -> int:
        return sum(
            order.scenario_applied
            for order in self.orders
        )

    @property
    def baseline_at_risk(self) -> int:
        return sum(
            order.baseline_predicted_late
            for order in self.orders
        )

    @property
    def simulated_at_risk(self) -> int:
        return sum(
            order.simulated_predicted_late
            for order in self.orders
        )

    @property
    def at_risk_delta(self) -> int:
        return (
            self.simulated_at_risk
            - self.baseline_at_risk
        )

    @property
    def newly_at_risk(self) -> int:
        return sum(
            order.became_at_risk
            for order in self.orders
        )

    @property
    def recovered_from_risk(self) -> int:
        return sum(
            order.recovered_from_risk
            for order in self.orders
        )

    @property
    def baseline_average_probability(self) -> float:
        if not self.orders:
            return 0.0

        return fmean(
            order.baseline_probability
            for order in self.orders
        )

    @property
    def simulated_average_probability(self) -> float:
        if not self.orders:
            return 0.0

        return fmean(
            order.simulated_probability
            for order in self.orders
        )

    @property
    def average_probability_delta(self) -> float:
        return (
            self.simulated_average_probability
            - self.baseline_average_probability
        )

    @property
    def baseline_risk_rate(self) -> float:
        if not self.orders:
            return 0.0

        return (
            self.baseline_at_risk
            / self.total_orders
        )

    @property
    def simulated_risk_rate(self) -> float:
        if not self.orders:
            return 0.0

        return (
            self.simulated_at_risk
            / self.total_orders
        )

    def by_city(
        self,
    ) -> tuple[CitySimulationState, ...]:
        grouped: dict[
            str,
            list[SimulatedOrderState],
        ] = {}

        for order in self.orders:
            grouped.setdefault(
                order.city,
                [],
            ).append(order)

        result: list[CitySimulationState] = []

        for city, orders in sorted(
            grouped.items()
        ):
            scenario_applies = (
                self.scenario.applies_to_city(
                    city
                )
            )

            demand_multiplier = (
                self.scenario.demand_multiplier
                if scenario_applies
                else 1.0
            )

            result.append(
                CitySimulationState(
                    city=city,
                    baseline_orders=len(orders),
                    simulated_demand_units=(
                        len(orders)
                        * demand_multiplier
                    ),
                    baseline_at_risk=sum(
                        order.baseline_predicted_late
                        for order in orders
                    ),
                    simulated_at_risk=sum(
                        order.simulated_predicted_late
                        for order in orders
                    ),
                    baseline_average_probability=fmean(
                        order.baseline_probability
                        for order in orders
                    ),
                    simulated_average_probability=fmean(
                        order.simulated_probability
                        for order in orders
                    ),
                )
            )

        return tuple(result)


def _scale_probability(
    probability: float,
    *,
    pressure_factor: float,
    stress_strength: float,
) -> float:
    """
    Applique une translation sur les log-odds.

    Cette transformation est monotone et conserve
    strictement l'intervalle [0, 1].
    """

    if probability <= 0.0:
        return 0.0

    if probability >= 1.0:
        return 1.0

    if stress_strength == 0.0:
        return probability

    logit = (
        log(probability)
        - log1p(-probability)
    )

    shift = (
        stress_strength
        * log(pressure_factor)
    )

    shifted_logit = logit + shift

    if shifted_logit >= 0:
        return 1.0 / (
            1.0 + exp(-shifted_logit)
        )

    exponential = exp(shifted_logit)

    return exponential / (
        1.0 + exponential
    )


def _simulate_order(
    order: TwinOrderState,
    scenario: SimulationScenario,
) -> SimulatedOrderState:
    applies = scenario.applies_to_city(
        order.city
    )

    probability = (
        _scale_probability(
            order.delay_probability,
            pressure_factor=(
                scenario.pressure_factor
            ),
            stress_strength=(
                scenario.stress_strength
            ),
        )
        if applies
        else order.delay_probability
    )

    return SimulatedOrderState(
        order_id=order.order_id,
        city=order.city,
        model_version=order.model_version,
        threshold=order.threshold,
        baseline_probability=(
            order.delay_probability
        ),
        simulated_probability=probability,
        baseline_predicted_late=(
            order.predicted_late
        ),
        simulated_predicted_late=(
            probability >= order.threshold
        ),
        scenario_applied=applies,
    )


@dataclass(frozen=True, slots=True)
class SimulationEngine:
    """Moteur deterministe de scenarios du Digital Twin."""

    def run(
        self,
        snapshot: DigitalTwinSnapshot,
        scenario: SimulationScenario,
        *,
        generated_at: datetime | None = None,
    ) -> SimulationResult:
        timestamp = generated_at or datetime.now(UTC)

        return SimulationResult(
            scenario=scenario,
            generated_at=timestamp,
            baseline_snapshot_generated_at=(
                snapshot.generated_at
            ),
            orders=tuple(
                _simulate_order(
                    order,
                    scenario,
                )
                for order in snapshot.orders
            ),
        )