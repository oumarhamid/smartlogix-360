from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DeliveryAcceptedEvent(BaseModel):
    """Evenement de livraison accepte au temps T0."""

    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    event_type: Literal["delivery_accepted"] = "delivery_accepted"

    # event_time = instant de publication/replay.
    event_time: datetime

    # source_event_time = vrai temps metier T0 = accept_timestamp.
    source_event_time: datetime

    order_id: int = Field(ge=0)
    region_id: int = Field(ge=0)
    city: str = Field(min_length=1)
    courier_id: int = Field(ge=0)
    aoi_id: int = Field(ge=0)
    aoi_type: int = Field(ge=0)

    sla_minutes: float = Field(gt=0)

    accept_gps_lng: float | None = None
    accept_gps_lat: float | None = None
    accept_gps_valid: bool

    @field_validator(
        "event_time",
        "source_event_time",
        mode="before",
    )
    @classmethod
    def normalize_datetime(
        cls,
        value: Any,
    ) -> datetime:
        """Normalise les timestamps en UTC."""

        return _as_datetime(value)

    @classmethod
    def from_gold_record(
        cls,
        record: Mapping[str, Any],
    ) -> DeliveryAcceptedEvent:
        """
        Construit l'evenement T0 depuis une ligne Gold LaDe.

        Pour les donnees historiques de replay, accept_timestamp
        peut etre reconstruit a partir de delivery_timestamp et
        delivery_duration_minutes lorsque le Gold compact ne
        contient pas directement accept_timestamp.

        Les informations post-livraison utilisees pour cette
        reconstruction ne sont jamais publiees dans l'evenement.
        """

        source_event_time = _resolve_accept_timestamp(
            record
        )

        return cls(
            event_id=uuid4(),
            event_type="delivery_accepted",
            event_time=datetime.now(UTC),
            source_event_time=source_event_time,
            order_id=int(record["order_id"]),
            region_id=int(record["region_id"]),
            city=str(record["city"]),
            courier_id=int(record["courier_id"]),
            aoi_id=int(record["aoi_id"]),
            aoi_type=int(record["aoi_type"]),
            sla_minutes=float(record["sla_minutes"]),
            accept_gps_lng=_as_optional_float(
                record.get("accept_gps_lng")
            ),
            accept_gps_lat=_as_optional_float(
                record.get("accept_gps_lat")
            ),
            accept_gps_valid=_resolve_accept_gps_valid(
                record
            ),
        )


def _resolve_accept_timestamp(
    record: Mapping[str, Any],
) -> datetime:
    """
    Retourne accept_timestamp ou le reconstruit pour le replay.

    La reconstruction historique est :

        accept_timestamp =
            delivery_timestamp - delivery_duration_minutes

    Une duree negative ou non finie est consideree invalide.
    """

    accept_timestamp = record.get(
        "accept_timestamp"
    )

    if not _is_missing(accept_timestamp):
        return _as_datetime(
            accept_timestamp
        )

    delivery_timestamp = record.get(
        "delivery_timestamp"
    )
    duration_value = record.get(
        "delivery_duration_minutes"
    )

    if _is_missing(delivery_timestamp):
        raise ValueError(
            "Impossible de reconstruire accept_timestamp: "
            "delivery_timestamp manquant"
        )

    if _is_missing(duration_value):
        raise ValueError(
            "Impossible de reconstruire accept_timestamp: "
            "delivery_duration_minutes manquant"
        )

    try:
        duration_minutes = float(
            duration_value
        )
    except (TypeError, ValueError) as error:
        raise ValueError(
            "delivery_duration_minutes invalide"
        ) from error

    if not math.isfinite(
        duration_minutes
    ):
        raise ValueError(
            "delivery_duration_minutes non finie"
        )

    if duration_minutes < 0:
        raise ValueError(
            "delivery_duration_minutes negative"
        )

    return _as_datetime(
        delivery_timestamp
    ) - timedelta(
        minutes=duration_minutes
    )


def _resolve_accept_gps_valid(
    record: Mapping[str, Any],
) -> bool:
    """
    Retourne accept_gps_valid ou le reconstruit.

    Une paire GPS est valide uniquement si les deux coordonnees
    sont presentes et dans leurs plages geographiques.
    """

    explicit_value = record.get(
        "accept_gps_valid"
    )

    if not _is_missing(
        explicit_value
    ):
        return bool(
            explicit_value
        )

    longitude = _as_optional_float(
        record.get(
            "accept_gps_lng"
        )
    )

    latitude = _as_optional_float(
        record.get(
            "accept_gps_lat"
        )
    )

    if (
        longitude is None
        or latitude is None
    ):
        return False

    return (
        -180.0 <= longitude <= 180.0
        and -90.0 <= latitude <= 90.0
    )


def _as_datetime(
    value: Any,
) -> datetime:
    """Convertit une valeur datetime compatible en UTC."""

    if hasattr(
        value,
        "to_pydatetime",
    ):
        value = value.to_pydatetime()

    if isinstance(
        value,
        str,
    ):
        try:
            value = datetime.fromisoformat(
                value.replace(
                    "Z",
                    "+00:00",
                )
            )
        except ValueError as error:
            raise ValueError(
                f"Timestamp invalide: {value!r}"
            ) from error

    if not isinstance(
        value,
        datetime,
    ):
        raise TypeError(
            "Une valeur datetime est attendue"
        )

    if value.tzinfo is None:
        return value.replace(
            tzinfo=UTC
        )

    return value.astimezone(
        UTC
    )


def _as_optional_float(
    value: Any,
) -> float | None:
    """Convertit une valeur numerique optionnelle."""

    if _is_missing(
        value
    ):
        return None

    try:
        result = float(
            value
        )
    except (TypeError, ValueError):
        return None

    if not math.isfinite(
        result
    ):
        return None

    return result


def _is_missing(
    value: Any,
) -> bool:
    """Detecte les valeurs None, NaN et NaT usuelles."""

    if value is None:
        return True

    if isinstance(
        value,
        float,
    ):
        return math.isnan(
            value
        )

    # pandas.NaT et valeurs similaires.
    try:
        missing = value != value
    except Exception:
        return False

    try:
        return bool(
            missing
        )
    except Exception:
        return False