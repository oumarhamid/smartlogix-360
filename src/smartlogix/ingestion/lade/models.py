from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class LaDeRemoteFile:
    """Métadonnées d'un fichier disponible dans le dépôt distant LaDe."""

    path: str
    category: str
    size_bytes: int | None

    @property
    def size_megabytes(self) -> float | None:
        """Retourne la taille du fichier en mégaoctets."""

        if self.size_bytes is None:
            return None

        return round(self.size_bytes / (1024**2), 3)

    @property
    def extension(self) -> str:
        """Retourne l'extension du fichier en minuscules."""

        filename = self.path.rsplit("/", maxsplit=1)[-1]

        if "." not in filename:
            return ""

        return f".{filename.rsplit('.', maxsplit=1)[-1].lower()}"

    def to_dict(self) -> dict[str, Any]:
        """Convertit les métadonnées en dictionnaire sérialisable."""

        data = asdict(self)
        data["size_megabytes"] = self.size_megabytes
        data["extension"] = self.extension

        return data


@dataclass(frozen=True, slots=True)
class LaDeRepositoryInventory:
    """Inventaire des fichiers disponibles dans le dépôt LaDe."""

    repository_id: str
    revision: str
    commit_sha: str
    last_modified: datetime | None
    inspected_at: datetime
    files: tuple[LaDeRemoteFile, ...]

    @property
    def file_count(self) -> int:
        """Retourne le nombre total de fichiers."""

        return len(self.files)

    @property
    def known_total_size_bytes(self) -> int:
        """Calcule la taille totale des fichiers dont la taille est connue."""

        return sum(file.size_bytes or 0 for file in self.files)

    @property
    def known_total_size_gigabytes(self) -> float:
        """Retourne la taille totale connue en gigaoctets."""

        return round(self.known_total_size_bytes / (1024**3), 3)

    @property
    def files_with_unknown_size_count(self) -> int:
        """Compte les fichiers dont la taille distante est inconnue."""

        return sum(file.size_bytes is None for file in self.files)

    def category_counts(self) -> dict[str, int]:
        """Retourne le nombre de fichiers par catégorie."""

        counts: dict[str, int] = {}

        for remote_file in self.files:
            counts[remote_file.category] = (
                counts.get(remote_file.category, 0) + 1
            )

        return dict(sorted(counts.items()))

    def extension_counts(self) -> dict[str, int]:
        """Retourne le nombre de fichiers par extension."""

        counts: dict[str, int] = {}

        for remote_file in self.files:
            extension = remote_file.extension or "no_extension"
            counts[extension] = counts.get(extension, 0) + 1

        return dict(sorted(counts.items()))

    def files_by_category(
        self,
        category: str,
    ) -> tuple[LaDeRemoteFile, ...]:
        """Filtre les fichiers selon leur catégorie."""

        return tuple(
            remote_file
            for remote_file in self.files
            if remote_file.category == category
        )

    def to_dict(self) -> dict[str, Any]:
        """Convertit l'inventaire en dictionnaire sérialisable."""

        return {
            "repository_id": self.repository_id,
            "revision": self.revision,
            "commit_sha": self.commit_sha,
            "last_modified": (
                self.last_modified.isoformat()
                if self.last_modified is not None
                else None
            ),
            "inspected_at": self.inspected_at.isoformat(),
            "file_count": self.file_count,
            "known_total_size_bytes": self.known_total_size_bytes,
            "known_total_size_gigabytes": (
                self.known_total_size_gigabytes
            ),
            "files_with_unknown_size_count": (
                self.files_with_unknown_size_count
            ),
            "category_counts": self.category_counts(),
            "extension_counts": self.extension_counts(),
            "files": [
                remote_file.to_dict()
                for remote_file in self.files
            ],
        }