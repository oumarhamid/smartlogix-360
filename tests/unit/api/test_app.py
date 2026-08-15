from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from smartlogix.api.app import (
    app,
    get_repository,
)
from smartlogix.digital_twin.models import (
    DigitalTwinSnapshot,
    TwinOrderState,
)


def make_order(
    *,
    order_id: int,
    city: str,
    probability: float,
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
        predicted_late=(
            probability >= threshold
        ),
        threshold=threshold,
        model_name="lightgbm",
        model_version="lightgbm-delay-v1",
        courier_prev_day_available=True,
        city_prev_day_available=True,
        alert_active=(
            probability >= threshold
        ),
        updated_at=timestamp,
    )


def make_snapshot() -> DigitalTwinSnapshot:
    return DigitalTwinSnapshot(
        generated_at=datetime.now(UTC),
        orders=(
            make_order(
                order_id=1,
                city="Jilin",
                probability=0.20,
            ),
            make_order(
                order_id=2,
                city="Jilin",
                probability=0.40,
            ),
            make_order(
                order_id=3,
                city="Yantai",
                probability=0.10,
            ),
            make_order(
                order_id=4,
                city="Yantai",
                probability=0.30,
            ),
        ),
    )


@dataclass
class FakeRepository:
    snapshot: DigitalTwinSnapshot
    load_count: int = 0

    def load_snapshot(
        self,
    ) -> DigitalTwinSnapshot:
        self.load_count += 1
        return self.snapshot


@pytest.fixture
def repository() -> FakeRepository:
    return FakeRepository(
        snapshot=make_snapshot()
    )


@pytest.fixture
def client(
    repository: FakeRepository,
) -> TestClient:
    app.dependency_overrides[
        get_repository
    ] = lambda: repository

    with TestClient(app) as test_client:
        yield test_client

    app.dependency_overrides.clear()


def test_health(
    client: TestClient,
) -> None:
    response = client.get("/health")

    assert response.status_code == 200

    assert response.json() == {
        "status": "ok",
        "service": (
            "smartlogix-digital-twin-api"
        ),
        "version": "1.0.0",
    }


def test_get_twin_state(
    client: TestClient,
) -> None:
    response = client.get(
        "/api/v1/twin/state"
    )

    assert response.status_code == 200

    payload = response.json()

    assert payload["summary"]["total_orders"] == 4
    assert payload["summary"]["at_risk_orders"] == 2
    assert len(payload["cities"]) == 2


def test_state_loads_repository(
    client: TestClient,
    repository: FakeRepository,
) -> None:
    client.get("/api/v1/twin/state")

    assert repository.load_count == 1


def test_simulation_endpoint(
    client: TestClient,
) -> None:
    response = client.post(
        "/api/v1/twin/simulations",
        json={
            "name": "demand+20",
            "demand_multiplier": 1.2,
        },
    )

    assert response.status_code == 200

    payload = response.json()

    assert (
        payload["scenario"]["name"]
        == "demand+20"
    )

    assert (
        payload["scenario"]["pressure_factor"]
        == pytest.approx(1.2)
    )

    assert (
        payload["summary"][
            "simulated_average_probability"
        ]
        > payload["summary"][
            "baseline_average_probability"
        ]
    )


def test_simulation_rejects_invalid_multiplier(
    client: TestClient,
) -> None:
    response = client.post(
        "/api/v1/twin/simulations",
        json={
            "name": "invalid",
            "demand_multiplier": 0,
        },
    )

    assert response.status_code == 422


def test_experiment_endpoint(
    client: TestClient,
) -> None:
    response = client.post(
        "/api/v1/twin/experiments",
        json={
            "scenarios": [
                {
                    "name": "baseline",
                },
                {
                    "name": "stress",
                    "demand_multiplier": 1.5,
                },
            ]
        },
    )

    assert response.status_code == 200

    payload = response.json()

    assert payload["best_scenario"] == "baseline"
    assert payload["worst_scenario"] == "stress"

    assert len(payload["comparisons"]) == 2


def test_experiment_rejects_empty_campaign(
    client: TestClient,
) -> None:
    response = client.post(
        "/api/v1/twin/experiments",
        json={
            "scenarios": [],
        },
    )

    assert response.status_code == 422


def test_optimization_endpoint(
    client: TestClient,
) -> None:
    response = client.post(
        "/api/v1/twin/optimizations",
        json={
            "name": "demand50",
            "demand_multiplier": 1.5,
            "capacity_min_multiplier": 1.0,
            "capacity_max_multiplier": 1.5,
            "capacity_step": 0.05,
            "budget": 0.25,
        },
    )

    assert response.status_code == 200

    payload = response.json()

    reference = payload["reference"]
    recommended = payload["recommended"]
    decision = payload["decision"]

    assert (
        recommended["intervention_cost"]
        <= 0.25
    )

    assert (
        recommended["simulated_at_risk"]
        <= reference["simulated_at_risk"]
    )

    assert decision["risk_reduction"] >= 0

    assert (
        decision["risk_delta_vs_baseline"]
        == (
            recommended["simulated_at_risk"]
            - payload["baseline"]["at_risk"]
        )
    )


def test_optimization_rejects_negative_budget(
    client: TestClient,
) -> None:
    response = client.post(
        "/api/v1/twin/optimizations",
        json={
            "name": "invalid",
            "budget": -1,
        },
    )

    assert response.status_code == 422


def test_openapi_exposes_twin_routes(
    client: TestClient,
) -> None:
    response = client.get(
        "/openapi.json"
    )

    assert response.status_code == 200

    paths = response.json()["paths"]

    assert "/api/v1/twin/state" in paths

    assert (
        "/api/v1/twin/simulations"
        in paths
    )

    assert (
        "/api/v1/twin/experiments"
        in paths
    )

    assert (
        "/api/v1/twin/optimizations"
        in paths
    )