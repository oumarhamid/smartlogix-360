from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from smartlogix.ingestion.lade import (
    LaDeRemoteFile,
    LaDeRepositoryInspectionError,
    LaDeRepositoryInspector,
)


class FakeHfApi:
    """Simulation minimale de l'API Hugging Face."""

    def __init__(
        self,
        dataset_info_result: Any | None = None,
        error: Exception | None = None,
    ) -> None:
        self.dataset_info_result = dataset_info_result
        self.error = error
        self.calls: list[dict[str, Any]] = []

    def dataset_info(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)

        if self.error is not None:
            raise self.error

        return self.dataset_info_result


def test_remote_file_properties() -> None:
    remote_file = LaDeRemoteFile(
        path="delivery/delivery_sh.csv",
        category="delivery",
        size_bytes=1_048_576,
    )

    assert remote_file.extension == ".csv"
    assert remote_file.size_megabytes == 1.0

    serialized_file = remote_file.to_dict()

    assert serialized_file["path"] == "delivery/delivery_sh.csv"
    assert serialized_file["category"] == "delivery"
    assert serialized_file["size_bytes"] == 1_048_576
    assert serialized_file["size_megabytes"] == 1.0
    assert serialized_file["extension"] == ".csv"


def test_classify_path() -> None:
    assert (
        LaDeRepositoryInspector.classify_path(
            "delivery/delivery_sh.csv"
        )
        == "delivery"
    )
    assert (
        LaDeRepositoryInspector.classify_path(
            "pickup/pickup_sh.csv"
        )
        == "pickup"
    )
    assert (
        LaDeRepositoryInspector.classify_path(
            "road-network/roads.csv"
        )
        == "road_network"
    )
    assert (
        LaDeRepositoryInspector.classify_path(
            "data_with_trajectory_20s/trajectory.pkl.xz"
        )
        == "trajectory"
    )
    assert (
        LaDeRepositoryInspector.classify_path(
            "delivery_five_cities.csv"
        )
        == "trajectory"
    )
    assert (
        LaDeRepositoryInspector.classify_path(
            "pickup_five_cities.csv"
        )
        == "trajectory"
    )
    assert (
        LaDeRepositoryInspector.classify_path(
            "courier_detailed_trajectory.zip"
        )
        == "trajectory"
    )
    assert (
        LaDeRepositoryInspector.classify_path(
            "img/delivery_process.png"
        )
        == "documentation_asset"
    )
    assert (
        LaDeRepositoryInspector.classify_path("README.md")
        == "documentation"
    )
    assert (
        LaDeRepositoryInspector.classify_path(
            "dataset_infos.json"
        )
        == "metadata"
    )
    assert (
        LaDeRepositoryInspector.classify_path(
            "unknown/file.bin"
        )
        == "other"
    )


def test_inspect_repository_builds_inventory() -> None:
    last_modified = datetime(
        2026,
        8,
        3,
        0,
        0,
        tzinfo=UTC,
    )

    fake_dataset_info = SimpleNamespace(
        sha="abc123def456",
        last_modified=last_modified,
        siblings=[
            SimpleNamespace(
                rfilename="delivery/delivery_sh.csv",
                size=1_048_576,
                lfs=None,
            ),
            SimpleNamespace(
                rfilename="road-network/roads.csv",
                size=None,
                lfs={"size": 2_048},
            ),
            SimpleNamespace(
                rfilename="README.md",
                size=None,
                lfs=None,
            ),
        ],
    )

    fake_api = FakeHfApi(
        dataset_info_result=fake_dataset_info
    )

    inspector = LaDeRepositoryInspector(
        repository_id="Cainiao-AI/LaDe",
        revision="main",
        api=fake_api,
    )

    inventory = inspector.inspect()

    assert inventory.repository_id == "Cainiao-AI/LaDe"
    assert inventory.revision == "main"
    assert inventory.commit_sha == "abc123def456"
    assert inventory.last_modified == last_modified
    assert inventory.file_count == 3

    assert inventory.category_counts() == {
        "delivery": 1,
        "documentation": 1,
        "road_network": 1,
    }

    assert inventory.extension_counts() == {
        ".csv": 2,
        ".md": 1,
    }

    assert inventory.known_total_size_bytes == 1_050_624
    assert inventory.files_with_unknown_size_count == 1

    assert fake_api.calls == [
        {
            "repo_id": "Cainiao-AI/LaDe",
            "revision": "main",
            "files_metadata": True,
            "token": False,
        }
    ]


def test_write_manifest_creates_valid_json(
    tmp_path: Path,
) -> None:
    fake_dataset_info = SimpleNamespace(
        sha="abc123",
        last_modified="2026-08-03T00:00:00Z",
        siblings=[
            SimpleNamespace(
                rfilename="delivery/delivery_sh.csv",
                size=100,
                lfs=None,
            )
        ],
    )

    inspector = LaDeRepositoryInspector(
        repository_id="Cainiao-AI/LaDe",
        api=FakeHfApi(
            dataset_info_result=fake_dataset_info
        ),
    )

    inventory = inspector.inspect()
    output_path = tmp_path / "remote_manifest.json"

    returned_path = inspector.write_manifest(
        inventory=inventory,
        output_path=output_path,
    )

    assert returned_path == output_path
    assert output_path.exists()
    assert not (
        tmp_path / "remote_manifest.json.tmp"
    ).exists()

    manifest = json.loads(
        output_path.read_text(encoding="utf-8")
    )

    assert manifest["repository_id"] == "Cainiao-AI/LaDe"
    assert manifest["commit_sha"] == "abc123"
    assert manifest["file_count"] == 1
    assert manifest["category_counts"] == {
        "delivery": 1
    }
    assert manifest["files"][0]["path"] == (
        "delivery/delivery_sh.csv"
    )
    assert manifest["files"][0]["extension"] == ".csv"


def test_inspection_error_is_explicit() -> None:
    inspector = LaDeRepositoryInspector(
        repository_id="Cainiao-AI/LaDe",
        api=FakeHfApi(
            error=ConnectionError("network unavailable")
        ),
    )

    with pytest.raises(
        LaDeRepositoryInspectionError,
        match="Impossible d'inspecter le dépôt LaDe",
    ):
        inspector.inspect()