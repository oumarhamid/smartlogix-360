from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DeliveryEvent(BaseModel):
    """Evenement logistique normalise publie dans Kafka."""

    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1)
    event_type: Literal["delivery_update"] = "delivery_update"
    event_time: datetime
    source_event_time: datetime

    order_id: int = Field(ge=0)
    region_id: int = Field(ge=0)
    city: str = Field(min_length=1)
    courier_id: int = Field(ge=0)
    aoi_id: int = Field(ge=0)
    aoi_type: int = Field(ge=0)

    delivery_duration_minutes: float | None = Field(default=None, ge=0)
    sla_minutes: float = Field(gt=0)
    is_within_sla: bool
    is_late_delivery: bool
    is_quality_warning: bool

    accept_gps_lng: float | None = None
    accept_gps_lat: float | None = None
    delivery_gps_lng: float | None = None
    delivery_gps_lat: float | None = None

    @field_validator("event_time", "source_event_time")
    @classmethod
    def ensure_utc(cls, value: datetime) -> datetime:
        """Normalise les horodatages en UTC."""

        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    @classmethod
    def from_gold_record(
        cls,
        record: Mapping[str, Any],
        *,
        event_time: datetime | None = None,
    ) -> DeliveryEvent:
        """Construit un evenement a partir d'une ligne Gold LaDe."""

        source_event_time = _as_datetime(record["delivery_timestamp"])

        return cls(
            event_id=str(uuid4()),
            event_time=event_time or datetime.now(UTC),
            source_event_time=source_event_time,
            order_id=int(record["order_id"]),
            region_id=int(record["region_id"]),
            city=str(record["city"]).strip(),
            courier_id=int(record["courier_id"]),
            aoi_id=int(record["aoi_id"]),
            aoi_type=int(record["aoi_type"]),
            delivery_duration_minutes=_as_optional_float(record.get("delivery_duration_minutes")),
            sla_minutes=float(record["sla_minutes"]),
            is_within_sla=bool(record["is_within_sla"]),
            is_late_delivery=bool(record["is_late_delivery"]),
            is_quality_warning=bool(record["is_quality_warning"]),
            accept_gps_lng=_as_optional_float(record.get("accept_gps_lng")),
            accept_gps_lat=_as_optional_float(record.get("accept_gps_lat")),
            delivery_gps_lng=_as_optional_float(record.get("delivery_gps_lng")),
            delivery_gps_lat=_as_optional_float(record.get("delivery_gps_lat")),
        )


def _as_datetime(value: Any) -> datetime:
    """Convertit une valeur compatible datetime en objet datetime."""

    if isinstance(value, datetime):
        return value

    to_pydatetime = getattr(value, "to_pydatetime", None)
    if callable(to_pydatetime):
        converted = to_pydatetime()
        if isinstance(converted, datetime):
            return converted

    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    raise TypeError(f"Horodatage non supporte: {type(value).__name__}")


def _as_optional_float(value: Any) -> float | None:
    """Convertit une valeur numerique nullable en float Python."""

    if value is None:
        return None

    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None

    if numeric != numeric:  # NaN
        return None

    return numeric
