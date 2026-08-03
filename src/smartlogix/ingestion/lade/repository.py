from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from huggingface_hub import HfApi

from smartlogix.common import get_logger
from smartlogix.ingestion.lade.models import (
    LaDeRemoteFile,
    LaDeRepositoryInventory,
)

logger = get_logger(__name__)


class DatasetInfoClient(Protocol):
    """Contrat minimal nécessaire pour interroger Hugging Face."""

    def dataset_info(
        self,
        *,
        repo_id: str,
        revision: str | None = None,
        files_metadata: bool = False,
        token: bool | str | None = None,
    ) -> Any:
        """Retourne les informations d'un dépôt de dataset."""


class LaDeRepositoryInspectionError(RuntimeError):
    """Erreur rencontrée pendant l'inspection du dépôt LaDe."""


class LaDeRepositoryInspector:
    """Inspecte le dépôt distant LaDe sans télécharger les datasets."""

    def __init__(
        self,
        repository_id: str,
        revision: str = "main",
        api: DatasetInfoClient | None = None,
    ) -> None:
        self.repository_id = repository_id
        self.revision = revision
        self.api = api or HfApi()

    @staticmethod
    def classify_path(path: str) -> str:
        """Classe un fichier distant selon son emplacement et son rôle."""

        normalized_path = path.replace("\\", "/").lower()
        filename = normalized_path.rsplit("/", maxsplit=1)[-1]

        if normalized_path.startswith("delivery/"):
            return "delivery"

        if normalized_path.startswith("pickup/"):
            return "pickup"

        if normalized_path.startswith("road-network/"):
            return "road_network"

        if normalized_path.startswith("data_with_trajectory_20s/"):
            return "trajectory"

        if filename in {
            "courier_detailed_trajectory.zip",
            "delivery_five_cities.csv",
            "pickup_five_cities.csv",
        }:
            return "trajectory"

        if normalized_path.startswith("img/"):
            return "documentation_asset"

        if normalized_path.endswith(".md"):
            return "documentation"

        if normalized_path.endswith(
            (
                ".json",
                ".yaml",
                ".yml",
                ".txt",
                ".gitattributes",
            )
        ):
            return "metadata"

        return "other"

    @staticmethod
    def _extract_path(sibling: Any) -> str | None:
        """Extrait le chemin relatif d'un fichier Hugging Face."""

        path = (
            getattr(sibling, "rfilename", None)
            or getattr(sibling, "path", None)
            or getattr(sibling, "filename", None)
        )

        return path if isinstance(path, str) else None

    @staticmethod
    def _extract_size(sibling: Any) -> int | None:
        """Extrait la taille normale ou LFS d'un fichier."""

        size = getattr(sibling, "size", None)

        if isinstance(size, int):
            return size

        lfs_metadata = getattr(sibling, "lfs", None)

        if isinstance(lfs_metadata, dict):
            lfs_size = lfs_metadata.get("size")

            if isinstance(lfs_size, int):
                return lfs_size

        if lfs_metadata is not None:
            lfs_size = getattr(lfs_metadata, "size", None)

            if isinstance(lfs_size, int):
                return lfs_size

        return None

    @staticmethod
    def _normalize_datetime(value: Any) -> datetime | None:
        """Normalise une date retournée par l'API Hugging Face."""

        if isinstance(value, datetime):
            return value

        if isinstance(value, str):
            normalized_value = value.replace("Z", "+00:00")

            try:
                return datetime.fromisoformat(normalized_value)
            except ValueError:
                logger.warning(
                    "lade_invalid_remote_datetime",
                    value=value,
                )

        return None

    def _build_files(
        self,
        siblings: Iterable[Any],
    ) -> tuple[LaDeRemoteFile, ...]:
        """Construit les modèles de fichiers distants."""

        remote_files: list[LaDeRemoteFile] = []

        for sibling in siblings:
            path = self._extract_path(sibling)

            if path is None:
                logger.warning(
                    "lade_remote_file_without_path",
                    repository_id=self.repository_id,
                )
                continue

            remote_files.append(
                LaDeRemoteFile(
                    path=path,
                    category=self.classify_path(path),
                    size_bytes=self._extract_size(sibling),
                )
            )

        return tuple(
            sorted(
                remote_files,
                key=lambda remote_file: remote_file.path.lower(),
            )
        )

    def inspect(self) -> LaDeRepositoryInventory:
        """Récupère l'inventaire distant du dépôt LaDe."""

        logger.info(
            "lade_repository_inspection_started",
            repository_id=self.repository_id,
            revision=self.revision,
        )

        try:
            dataset_info = self.api.dataset_info(
                repo_id=self.repository_id,
                revision=self.revision,
                files_metadata=True,
                token=False,
            )
        except Exception as error:
            logger.exception(
                "lade_repository_inspection_failed",
                repository_id=self.repository_id,
                revision=self.revision,
                error_type=type(error).__name__,
            )

            raise LaDeRepositoryInspectionError(
                "Impossible d'inspecter le dépôt LaDe "
                f"'{self.repository_id}' à la révision "
                f"'{self.revision}'."
            ) from error

        siblings = getattr(dataset_info, "siblings", None) or []
        commit_sha = getattr(dataset_info, "sha", None)

        if not isinstance(commit_sha, str) or not commit_sha:
            commit_sha = self.revision

        inventory = LaDeRepositoryInventory(
            repository_id=self.repository_id,
            revision=self.revision,
            commit_sha=commit_sha,
            last_modified=self._normalize_datetime(
                getattr(dataset_info, "last_modified", None)
            ),
            inspected_at=datetime.now(UTC),
            files=self._build_files(siblings),
        )

        logger.info(
            "lade_repository_inspection_completed",
            repository_id=self.repository_id,
            commit_sha=inventory.commit_sha,
            file_count=inventory.file_count,
            category_counts=inventory.category_counts(),
            known_total_size_bytes=inventory.known_total_size_bytes,
            unknown_size_count=(
                inventory.files_with_unknown_size_count
            ),
        )

        return inventory

    @staticmethod
    def write_manifest(
        inventory: LaDeRepositoryInventory,
        output_path: Path,
    ) -> Path:
        """Écrit atomiquement l'inventaire dans un manifeste JSON."""

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        temporary_path = output_path.with_name(
            f"{output_path.name}.tmp"
        )

        manifest_content = json.dumps(
            inventory.to_dict(),
            ensure_ascii=False,
            indent=2,
        )

        temporary_path.write_text(
            manifest_content,
            encoding="utf-8",
        )

        temporary_path.replace(output_path)

        logger.info(
            "lade_repository_manifest_written",
            output_path=str(output_path),
            file_count=inventory.file_count,
        )

        return output_path