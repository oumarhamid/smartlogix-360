from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from statistics import fmean


class TwinOperationalState(StrEnum):
    """Etat operationnel courant d'une commande dans le jumeau."""

    MONITORED = "monitored"
    AT_RISK = "at_risk"
    DELIVERED_ON_TIME = "delivered_on_time"
    DELIVERED_LATE = "delivered_late"


class TwinHistoryCoverage(StrEnum):
    """Disponibilite des historiques J-1 utilises par le modele."""

    NONE = "none"
    PARTIAL = "partial"
    FULL = "full"


class PredictionOutcome(StrEnum):
    """Comparaison entre prediction T0 et resultat observe."""

    PENDING_OBSERVATION = "pending_observation"
    TRUE_POSITIVE = "true_positive"
    TRUE_NEGATIVE = "true_negative"
    FALSE_POSITIVE = "false_positive"
    FALSE_NEGATIVE = "false_negative"


@dataclass(frozen=True, slots=True)
class ObservedDeliveryState:
    """Etat reel observe apres l'acceptation de la livraison."""

    event_id: str
    event_type: str

    event_time: datetime
    source_event_time: datetime

    aoi_type: int | None

    delivery_duration_minutes: float | None
    sla_minutes: float | None

    is_within_sla: bool | None
    is_late_delivery: bool | None
    is_quality_warning: bool | None

    accept_gps_lng: float | None
    accept_gps_lat: float | None

    delivery_gps_lng: float | None
    delivery_gps_lat: float | None

    kafka_partition: int | None
    kafka_offset: int | None
    kafka_timestamp: datetime | None

    updated_at: datetime

    def __post_init__(self) -> None:
        if not self.event_id.strip():
            raise ValueError(
                "event_id ne peut pas etre vide."
            )

        if not self.event_type.strip():
            raise ValueError(
                "event_type ne peut pas etre vide."
            )

        for name, value in (
            ("event_time", self.event_time),
            (
                "source_event_time",
                self.source_event_time,
            ),
            ("updated_at", self.updated_at),
        ):
            if value.tzinfo is None:
                raise ValueError(
                    f"{name} doit etre timezone-aware."
                )

        if (
            self.kafka_timestamp is not None
            and self.kafka_timestamp.tzinfo is None
        ):
            raise ValueError(
                "kafka_timestamp doit etre timezone-aware."
            )

        if (
            self.delivery_duration_minutes is not None
            and self.delivery_duration_minutes < 0
        ):
            raise ValueError(
                "delivery_duration_minutes "
                "doit etre positive ou nulle."
            )

        if (
            self.sla_minutes is not None
            and self.sla_minutes <= 0
        ):
            raise ValueError(
                "sla_minutes doit etre strictement positif."
            )

        if (
            self.is_within_sla is not None
            and self.is_late_delivery is not None
            and self.is_within_sla
            == self.is_late_delivery
        ):
            raise ValueError(
                "is_within_sla et is_late_delivery "
                "sont incoherents."
            )

        self._validate_longitude(
            "accept_gps_lng",
            self.accept_gps_lng,
        )
        self._validate_longitude(
            "delivery_gps_lng",
            self.delivery_gps_lng,
        )
        self._validate_latitude(
            "accept_gps_lat",
            self.accept_gps_lat,
        )
        self._validate_latitude(
            "delivery_gps_lat",
            self.delivery_gps_lat,
        )

    @staticmethod
    def _validate_longitude(
        name: str,
        value: float | None,
    ) -> None:
        if value is not None and not -180 <= value <= 180:
            raise ValueError(
                f"{name} doit etre comprise entre -180 et 180."
            )

    @staticmethod
    def _validate_latitude(
        name: str,
        value: float | None,
    ) -> None:
        if value is not None and not -90 <= value <= 90:
            raise ValueError(
                f"{name} doit etre comprise entre -90 et 90."
            )

    @property
    def has_delivery_outcome(self) -> bool:
        return self.is_late_delivery is not None


@dataclass(frozen=True, slots=True)
class TwinOrderState:
    """Etat courant observe et predictif d'une commande."""

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

    observed_state: ObservedDeliveryState | None = None

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
                "delay_probability doit etre comprise "
                "entre 0 et 1."
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
        if self.observed_state is not None:
            actual_late = (
                self.observed_state.is_late_delivery
            )

            if actual_late is True:
                return TwinOperationalState.DELIVERED_LATE

            if actual_late is False:
                return (
                    TwinOperationalState.DELIVERED_ON_TIME
                )

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

    @property
    def prediction_outcome(
        self,
    ) -> PredictionOutcome:
        if (
            self.observed_state is None
            or self.observed_state.is_late_delivery
            is None
        ):
            return PredictionOutcome.PENDING_OBSERVATION

        actual_late = (
            self.observed_state.is_late_delivery
        )

        if self.predicted_late and actual_late:
            return PredictionOutcome.TRUE_POSITIVE

        if not self.predicted_late and not actual_late:
            return PredictionOutcome.TRUE_NEGATIVE

        if self.predicted_late and not actual_late:
            return PredictionOutcome.FALSE_POSITIVE

        return PredictionOutcome.FALSE_NEGATIVE


@dataclass(frozen=True, slots=True)
class CityTwinState:
    """Vue agregee du jumeau pour une ville."""

    city: str

    total_orders: int
    at_risk_orders: int

    average_delay_probability: float
    maximum_delay_probability: float

    full_history_orders: int

    observed_orders: int
    actual_late_orders: int

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

    @property
    def observation_rate(self) -> float:
        if self.total_orders == 0:
            return 0.0

        return self.observed_orders / self.total_orders

    @property
    def actual_late_rate(self) -> float:
        if self.observed_orders == 0:
            return 0.0

        return (
            self.actual_late_orders
            / self.observed_orders
        )


@dataclass(frozen=True, slots=True)
class DigitalTwinSnapshot:
    """Snapshot coherent de l'etat du systeme logistique."""

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
                "Le snapshot contient des commandes "
                "dupliquees pour une meme version de modele."
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
    def observed_orders(self) -> int:
        return sum(
            order.observed_state is not None
            for order in self.orders
        )

    @property
    def evaluated_predictions(self) -> int:
        return sum(
            order.prediction_outcome
            != PredictionOutcome.PENDING_OBSERVATION
            for order in self.orders
        )

    @property
    def actual_late_orders(self) -> int:
        return sum(
            order.observed_state is not None
            and order.observed_state.is_late_delivery
            is True
            for order in self.orders
        )

    @property
    def true_positives(self) -> int:
        return self._count_outcome(
            PredictionOutcome.TRUE_POSITIVE
        )

    @property
    def true_negatives(self) -> int:
        return self._count_outcome(
            PredictionOutcome.TRUE_NEGATIVE
        )

    @property
    def false_positives(self) -> int:
        return self._count_outcome(
            PredictionOutcome.FALSE_POSITIVE
        )

    @property
    def false_negatives(self) -> int:
        return self._count_outcome(
            PredictionOutcome.FALSE_NEGATIVE
        )

    def _count_outcome(
        self,
        outcome: PredictionOutcome,
    ) -> int:
        return sum(
            order.prediction_outcome == outcome
            for order in self.orders
        )

    @property
    def prediction_accuracy(self) -> float:
        if self.evaluated_predictions == 0:
            return 0.0

        return (
            self.true_positives
            + self.true_negatives
        ) / self.evaluated_predictions

    @property
    def prediction_precision(self) -> float:
        denominator = (
            self.true_positives
            + self.false_positives
        )

        if denominator == 0:
            return 0.0

        return self.true_positives / denominator

    @property
    def prediction_recall(self) -> float:
        denominator = (
            self.true_positives
            + self.false_negatives
        )

        if denominator == 0:
            return 0.0

        return self.true_positives / denominator

    @property
    def risk_rate(self) -> float:
        if not self.orders:
            return 0.0

        return self.at_risk_orders / self.total_orders

    @property
    def observation_rate(self) -> float:
        if not self.orders:
            return 0.0

        return self.observed_orders / self.total_orders

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
                    observed_orders=sum(
                        order.observed_state is not None
                        for order in orders
                    ),
                    actual_late_orders=sum(
                        order.observed_state
                        is not None
                        and order.observed_state
                        .is_late_delivery
                        is True
                        for order in orders
                    ),
                )
            )

        return tuple(result)