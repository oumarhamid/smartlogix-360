from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from huggingface_hub import hf_hub_download

from smartlogix.common import get_logger

logger = get_logger(__name__)


class LaDeDownloadError(RuntimeError):
    """Erreur rencontrée pendant le téléchargement d'un fichier LaDe."""


class LaDeDownloadValidationError(LaDeDownloadError):
    """Le fichier téléchargé ne correspond pas aux métadonnées attendues."""


@dataclass(frozen=True, slots=True)
class LaDeDownloadedFile:
    """Résultat validé du téléchargement d'un fichier LaDe."""

    repository_id: str
    revision: str
    remote_path: str
    local_path: str
    size_bytes: int
    sha256: str
    downloaded_at: datetime

    @property
    def size_megabytes(self) -> float:
        """Retourne la taille du fichier en mégaoctets."""

        return round(self.size_bytes / (1024**2), 3)

    def to_dict(self) -> dict[str, Any]:
        """Convertit le résultat en dictionnaire sérialisable."""

        data = asdict(self)
        data["downloaded_at"] = self.downloaded_at.isoformat()
        data["size_megabytes"] = self.size_megabytes

        return data


class LaDeFileDownloader:
    """Télécharge et valide un fichier précis du dataset LaDe."""

    def __init__(
        self,
        repository_id: str,
        revision: str,
        destination_root: Path,
        download_function: Callable[..., str] = hf_hub_download,
    ) -> None:
        self.repository_id = repository_id
        self.revision = revision
        self.destination_root = destination_root
        self.download_function = download_function

    @staticmethod
    def calculate_sha256(
        file_path: Path,
        chunk_size: int = 1024 * 1024,
    ) -> str:
        """Calcule le SHA-256 d'un fichier sans le charger en mémoire."""

        digest = hashlib.sha256()

        with file_path.open("rb") as file_handle:
            while chunk := file_handle.read(chunk_size):
                digest.update(chunk)

        return digest.hexdigest()

    def download(
        self,
        remote_path: str,
        expected_size_bytes: int | None = None,
        force_download: bool = False,
    ) -> LaDeDownloadedFile:
        """Télécharge un fichier puis vérifie son existence et sa taille."""

        self.destination_root.mkdir(
            parents=True,
            exist_ok=True,
        )

        logger.info(
            "lade_file_download_started",
            repository_id=self.repository_id,
            revision=self.revision,
            remote_path=remote_path,
            expected_size_bytes=expected_size_bytes,
        )

        try:
            downloaded_path = self.download_function(
                repo_id=self.repository_id,
                repo_type="dataset",
                filename=remote_path,
                revision=self.revision,
                local_dir=str(self.destination_root),
                force_download=force_download,
                token=False,
            )
        except Exception as error:
            logger.exception(
                "lade_file_download_failed",
                repository_id=self.repository_id,
                revision=self.revision,
                remote_path=remote_path,
                error_type=type(error).__name__,
            )

            raise LaDeDownloadError(
                f"Impossible de télécharger le fichier '{remote_path}' "
                f"depuis le dépôt '{self.repository_id}'."
            ) from error

        local_path = Path(downloaded_path).resolve()

        if not local_path.is_file():
            raise LaDeDownloadValidationError(
                "Le téléchargement est terminé, mais le fichier local "
                f"est introuvable : {local_path}"
            )

        actual_size_bytes = local_path.stat().st_size

        if (
            expected_size_bytes is not None
            and actual_size_bytes != expected_size_bytes
        ):
            raise LaDeDownloadValidationError(
                f"Taille invalide pour '{remote_path}' : "
                f"{actual_size_bytes} octets reçus au lieu de "
                f"{expected_size_bytes} octets attendus."
            )

        sha256 = self.calculate_sha256(local_path)

        result = LaDeDownloadedFile(
            repository_id=self.repository_id,
            revision=self.revision,
            remote_path=remote_path,
            local_path=str(local_path),
            size_bytes=actual_size_bytes,
            sha256=sha256,
            downloaded_at=datetime.now(UTC),
        )

        logger.info(
            "lade_file_download_completed",
            repository_id=self.repository_id,
            revision=self.revision,
            remote_path=remote_path,
            local_path=str(local_path),
            size_bytes=actual_size_bytes,
            sha256=sha256,
        )

        return result

    @staticmethod
    def write_download_manifest(
        downloaded_file: LaDeDownloadedFile,
        output_path: Path,
    ) -> Path:
        """Enregistre atomiquement le manifeste du téléchargement."""

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        temporary_path = output_path.with_name(
            f"{output_path.name}.tmp"
        )

        content = json.dumps(
            downloaded_file.to_dict(),
            ensure_ascii=False,
            indent=2,
        )

        temporary_path.write_text(
            content,
            encoding="utf-8",
        )

        temporary_path.replace(output_path)

        logger.info(
            "lade_download_manifest_written",
            output_path=str(output_path),
            remote_path=downloaded_file.remote_path,
        )

        return output_path