from __future__ import annotations

from pydantic import BaseModel, Field


class SimulationRequest(BaseModel):
    name: str = Field(
        min_length=1,
        max_length=100,
    )

    demand_multiplier: float = Field(
        default=1.0,
        gt=0,
    )

    courier_capacity_multiplier: float = Field(
        default=1.0,
        gt=0,
    )

    sla_multiplier: float = Field(
        default=1.0,
        gt=0,
    )

    stress_strength: float = Field(
        default=1.0,
        ge=0,
    )

    target_city: str | None = None


class ExperimentRequest(BaseModel):
    scenarios: list[SimulationRequest] = Field(
        min_length=1,
    )


class OptimizationRequest(BaseModel):
    name: str = Field(
        min_length=1,
        max_length=100,
    )

    demand_multiplier: float = Field(
        default=1.0,
        gt=0,
    )

    capacity_min_multiplier: float = Field(
        default=1.0,
        ge=1.0,
    )

    capacity_max_multiplier: float = Field(
        default=1.5,
        ge=1.0,
    )

    capacity_step: float = Field(
        default=0.05,
        gt=0,
    )

    budget: float = Field(
        default=0.50,
        ge=0,
    )

    capacity_unit_cost: float = Field(
        default=1.0,
        gt=0,
    )

    max_risk_rate: float | None = Field(
        default=None,
        ge=0,
        le=1,
    )

    stress_strength: float = Field(
        default=1.0,
        ge=0,
    )

    target_city: str | None = None