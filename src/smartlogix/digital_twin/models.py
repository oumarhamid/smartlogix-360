from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from statistics import fmean


class TwinOperationalState(StrEnum):
    """Etat operationnel predictif d'une commande dans le jumeau."""

    MONITORED = "monitored"
    AT_RISK = "at_risk"


class TwinHistoryCoverage(StrEnum):
    """Disponibilite des historiques J-1 utilises par le modele."""

    NONE = "none"
    PARTIAL = "partial"
    FULL = "full"


@dataclass(frozen=True, slots=True)
class TwinOrderState:
    """Etat predictif courant d'une commande dans le jumeau numerique."""

    order_id: int
    source_event_time: datetime

    region_id: int
    city: str
    courier_id: int
    aoi_id: int

    delay_probability: float
    predicted_late: bool
    threshold: float

    model_name: str
    model_version: str

    courier_prev_day_available: bool
    city_prev_day_available: bool

    alert_active: bool
    updated_at: datetime

    def __post_init__(self) -> None:
        for name, value in (
            ("order_id", self.order_id),
            ("region_id", self.region_id),
            ("courier_id", self.courier_id),
            ("aoi_id", self.aoi_id),
        ):
            if value < 0:
                raise ValueError(
                    f"{name} doit etre positif ou nul."
                )

        if not self.city.strip():
            raise ValueError(
                "city ne peut pas etre vide."
            )

        if not 0.0 <= self.delay_probability <= 1.0:
            raise ValueError(
                "delay_probability doit etre comprise entre 0 et 1."
            )

        if not 0.0 <= self.threshold <= 1.0:
            raise ValueError(
                "threshold doit etre compris entre 0 et 1."
            )

        if not self.model_name.strip():
            raise ValueError(
                "model_name ne peut pas etre vide."
            )

        if not self.model_version.strip():
            raise ValueError(
                "model_version ne peut pas etre vide."
            )

        if self.source_event_time.tzinfo is None:
            raise ValueError(
                "source_event_time doit etre timezone-aware."
            )

        if self.updated_at.tzinfo is None:
            raise ValueError(
                "updated_at doit etre timezone-aware."
            )

        if self.predicted_late != (
            self.delay_probability >= self.threshold
        ):
            raise ValueError(
                "predicted_late est incoherent avec "
                "delay_probability et threshold."
            )

        if self.alert_active and not self.predicted_late:
            raise ValueError(
                "Une alerte active exige predicted_late=True."
            )

    @property
    def operational_state(
        self,
    ) -> TwinOperationalState:
        if self.predicted_late:
            return TwinOperationalState.AT_RISK

        return TwinOperationalState.MONITORED

    @property
    def history_coverage(
        self,
    ) -> TwinHistoryCoverage:
        available = (
            self.courier_prev_day_available,
            self.city_prev_day_available,
        )

        if all(available):
            return TwinHistoryCoverage.FULL

        if any(available):
            return TwinHistoryCoverage.PARTIAL

        return TwinHistoryCoverage.NONE


@dataclass(frozen=True, slots=True)
class CityTwinState:
    """Vue agregee du jumeau pour une ville."""

    city: str
    total_orders: int
    at_risk_orders: int
    average_delay_probability: float
    maximum_delay_probability: float
    full_history_orders: int

    @property
    def risk_rate(self) -> float:
        if self.total_orders == 0:
            return 0.0

        return self.at_risk_orders / self.total_orders

    @property
    def full_history_rate(self) -> float:
        if self.total_orders == 0:
            return 0.0

        return self.full_history_orders / self.total_orders


@dataclass(frozen=True, slots=True)
class DigitalTwinSnapshot:
    """Snapshot coherent de l'etat predictif du systeme logistique."""

    generated_at: datetime
    orders: tuple[TwinOrderState, ...]

    def __post_init__(self) -> None:
        if self.generated_at.tzinfo is None:
            raise ValueError(
                "generated_at doit etre timezone-aware."
            )

        order_keys = [
            (order.order_id, order.model_version)
            for order in self.orders
        ]

        if len(order_keys) != len(set(order_keys)):
            raise ValueError(
                "Le snapshot contient des commandes dupliquees "
                "pour une meme version de modele."
            )

    @property
    def total_orders(self) -> int:
        return len(self.orders)

    @property
    def at_risk_orders(self) -> int:
        return sum(
            order.predicted_late
            for order in self.orders
        )

    @property
    def active_alerts(self) -> int:
        return sum(
            order.alert_active
            for order in self.orders
        )

    @property
    def risk_rate(self) -> float:
        if not self.orders:
            return 0.0

        return self.at_risk_orders / self.total_orders

    @property
    def average_delay_probability(self) -> float:
        if not self.orders:
            return 0.0

        return fmean(
            order.delay_probability
            for order in self.orders
        )

    @property
    def maximum_delay_probability(self) -> float:
        if not self.orders:
            return 0.0

        return max(
            order.delay_probability
            for order in self.orders
        )

    @property
    def full_history_orders(self) -> int:
        return sum(
            order.history_coverage
            == TwinHistoryCoverage.FULL
            for order in self.orders
        )

    def by_city(self) -> tuple[CityTwinState, ...]:
        grouped: dict[str, list[TwinOrderState]] = {}

        for order in self.orders:
            grouped.setdefault(
                order.city,
                [],
            ).append(order)

        result: list[CityTwinState] = []

        for city, orders in sorted(grouped.items()):
            probabilities = [
                order.delay_probability
                for order in orders
            ]

            result.append(
                CityTwinState(
                    city=city,
                    total_orders=len(orders),
                    at_risk_orders=sum(
                        order.predicted_late
                        for order in orders
                    ),
                    average_delay_probability=fmean(
                        probabilities
                    ),
                    maximum_delay_probability=max(
                        probabilities
                    ),
                    full_history_orders=sum(
                        order.history_coverage
                        == TwinHistoryCoverage.FULL
                        for order in orders
                    ),
                )
            )

        return tuple(result)