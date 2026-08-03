from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from smartlogix.ingestion.lade import (
    LaDeDownloadedFile,
    LaDeDownloadError,
    LaDeDownloadValidationError,
    LaDeFileDownloader,
)


class FakeDownloadFunction:
    """Simule hf_hub_download sans utiliser Internet."""

    def __init__(
        self,
        content: bytes = b"smartlogix-test-data",
        error: Exception | None = None,
    ) -> None:
        self.content = content
        self.error = error
        self.calls: list[dict[str, Any]] = []

    def __call__(self, **kwargs: Any) -> str:
        self.calls.append(kwargs)

        if self.error is not None:
            raise self.error

        destination_root = Path(kwargs["local_dir"])
        destination_path = destination_root / kwargs["filename"]

        destination_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )
        destination_path.write_bytes(self.content)

        return str(destination_path)


def test_calculate_sha256(tmp_path: Path) -> None:
    file_path = tmp_path / "sample.csv"
    content = b"shipment_id,driver_id\nSHP-001,DRV-001\n"

    file_path.write_bytes(content)

    expected_sha256 = hashlib.sha256(content).hexdigest()

    assert (
        LaDeFileDownloader.calculate_sha256(file_path)
        == expected_sha256
    )


def test_download_validates_and_returns_metadata(
    tmp_path: Path,
) -> None:
    content = b"package_id,courier_id\nPKG-001,CR-001\n"
    fake_download = FakeDownloadFunction(content=content)

    downloader = LaDeFileDownloader(
        repository_id="Cainiao-AI/LaDe",
        revision="abc123",
        destination_root=tmp_path,
        download_function=fake_download,
    )

    downloaded_file = downloader.download(
        remote_path="delivery/delivery_jl.csv",
        expected_size_bytes=len(content),
    )

    local_path = Path(downloaded_file.local_path)

    assert local_path.exists()
    assert local_path.read_bytes() == content
    assert downloaded_file.repository_id == "Cainiao-AI/LaDe"
    assert downloaded_file.revision == "abc123"
    assert downloaded_file.remote_path == (
        "delivery/delivery_jl.csv"
    )
    assert downloaded_file.size_bytes == len(content)
    assert downloaded_file.sha256 == (
        hashlib.sha256(content).hexdigest()
    )

    assert fake_download.calls == [
        {
            "repo_id": "Cainiao-AI/LaDe",
            "repo_type": "dataset",
            "filename": "delivery/delivery_jl.csv",
            "revision": "abc123",
            "local_dir": str(tmp_path),
            "force_download": False,
            "token": False,
        }
    ]


def test_download_rejects_invalid_size(
    tmp_path: Path,
) -> None:
    fake_download = FakeDownloadFunction(
        content=b"incorrect-size"
    )

    downloader = LaDeFileDownloader(
        repository_id="Cainiao-AI/LaDe",
        revision="abc123",
        destination_root=tmp_path,
        download_function=fake_download,
    )

    with pytest.raises(
        LaDeDownloadValidationError,
        match="Taille invalide",
    ):
        downloader.download(
            remote_path="delivery/delivery_jl.csv",
            expected_size_bytes=999,
        )


def test_download_wraps_remote_error(
    tmp_path: Path,
) -> None:
    fake_download = FakeDownloadFunction(
        error=ConnectionError("network unavailable")
    )

    downloader = LaDeFileDownloader(
        repository_id="Cainiao-AI/LaDe",
        revision="abc123",
        destination_root=tmp_path,
        download_function=fake_download,
    )

    with pytest.raises(
        LaDeDownloadError,
        match="Impossible de télécharger le fichier",
    ):
        downloader.download(
            remote_path="delivery/delivery_jl.csv"
        )


def test_write_download_manifest(
    tmp_path: Path,
) -> None:
    downloaded_file = LaDeDownloadedFile(
        repository_id="Cainiao-AI/LaDe",
        revision="abc123",
        remote_path="delivery/delivery_jl.csv",
        local_path=str(
            tmp_path / "delivery" / "delivery_jl.csv"
        ),
        size_bytes=4_736_342,
        sha256="a" * 64,
        downloaded_at=datetime(
            2026,
            8,
            3,
            1,
            30,
            tzinfo=UTC,
        ),
    )

    manifest_path = (
        tmp_path
        / "_metadata"
        / "delivery_jl.download.json"
    )

    returned_path = (
        LaDeFileDownloader.write_download_manifest(
            downloaded_file=downloaded_file,
            output_path=manifest_path,
        )
    )

    assert returned_path == manifest_path
    assert manifest_path.exists()
    assert not manifest_path.with_name(
        f"{manifest_path.name}.tmp"
    ).exists()

    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )

    assert manifest["repository_id"] == "Cainiao-AI/LaDe"
    assert manifest["revision"] == "abc123"
    assert manifest["remote_path"] == (
        "delivery/delivery_jl.csv"
    )
    assert manifest["size_bytes"] == 4_736_342
    assert manifest["sha256"] == "a" * 64
    assert manifest["size_megabytes"] == 4.517