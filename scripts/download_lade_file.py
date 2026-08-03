from __future__ import annotations

import argparse
import json
from json import JSONDecodeError
from pathlib import Path
from typing import Any

from smartlogix.common import configure_logging
from smartlogix.config import get_settings
from smartlogix.ingestion.lade import (
    LaDeDownloadError,
    LaDeFileDownloader,
)

DEFAULT_REMOTE_PATH = "delivery/delivery_jl.csv"


def parse_arguments() -> argparse.Namespace:
    """Analyse les arguments de la ligne de commande."""

    parser = argparse.ArgumentParser(
        description=(
            "Télécharge et valide un fichier précis du dataset LaDe "
            "à partir de son manifeste distant."
        )
    )

    parser.add_argument(
        "--remote-path",
        default=DEFAULT_REMOTE_PATH,
        help="Chemin du fichier dans le dépôt Hugging Face.",
    )

    parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Chemin du manifeste distant LaDe.",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Force un nouveau téléchargement du fichier.",
    )

    return parser.parse_args()


def load_remote_manifest(manifest_path: Path) -> dict[str, Any]:
    """Charge et vérifie le manifeste distant."""

    if not manifest_path.is_file():
        raise FileNotFoundError(
            f"Le manifeste distant est introuvable : {manifest_path}"
        )

    try:
        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8")
        )
    except JSONDecodeError as error:
        raise ValueError(
            f"Le manifeste JSON est invalide : {manifest_path}"
        ) from error

    if not isinstance(manifest, dict):
        raise ValueError(
            "Le manifeste distant doit contenir un objet JSON."
        )

    required_fields = {
        "repository_id",
        "commit_sha",
        "files",
    }

    missing_fields = required_fields.difference(manifest)

    if missing_fields:
        missing_fields_text = ", ".join(
            sorted(missing_fields)
        )

        raise ValueError(
            "Le manifeste distant est incomplet. "
            f"Champs absents : {missing_fields_text}"
        )

    if not isinstance(manifest["files"], list):
        raise ValueError(
            "Le champ 'files' du manifeste doit être une liste."
        )

    return manifest


def find_remote_file(
    manifest: dict[str, Any],
    remote_path: str,
) -> dict[str, Any]:
    """Recherche un fichier précis dans le manifeste distant."""

    for remote_file in manifest["files"]:
        if (
            isinstance(remote_file, dict)
            and remote_file.get("path") == remote_path
        ):
            return remote_file

    raise ValueError(
        f"Le fichier '{remote_path}' est absent du manifeste."
    )


def build_download_manifest_path(
    metadata_directory: Path,
    remote_path: str,
) -> Path:
    """Construit le chemin du manifeste local de téléchargement."""

    normalized_name = remote_path.replace("\\", "/")
    normalized_name = normalized_name.replace("/", "__")

    return (
        metadata_directory
        / f"{normalized_name}.download.json"
    )


def print_download_summary(
    repository_id: str,
    revision: str,
    remote_path: str,
    local_path: str,
    size_bytes: int,
    size_megabytes: float,
    sha256: str,
    manifest_path: Path,
) -> None:
    """Affiche les résultats du téléchargement."""

    print()
    print("=" * 70)
    print("SMARTLOGIX 360 - TÉLÉCHARGEMENT LADE VALIDÉ")
    print("=" * 70)
    print(f"Dépôt             : {repository_id}")
    print(f"Commit utilisé    : {revision}")
    print(f"Fichier distant   : {remote_path}")
    print(f"Fichier local     : {local_path}")
    print(f"Taille            : {size_bytes} octets")
    print(f"Taille en Mo      : {size_megabytes}")
    print(f"SHA-256           : {sha256}")
    print(f"Manifeste local   : {manifest_path}")
    print("=" * 70)


def main() -> int:
    """Télécharge et valide un fichier LaDe."""

    configure_logging()

    arguments = parse_arguments()
    settings = get_settings()

    manifest_path = arguments.manifest or (
        settings.resolved_lade_data_dir
        / "_metadata"
        / "remote_manifest.json"
    )

    try:
        manifest = load_remote_manifest(manifest_path)

        remote_file = find_remote_file(
            manifest=manifest,
            remote_path=arguments.remote_path,
        )

        repository_id = str(manifest["repository_id"])
        revision = str(manifest["commit_sha"])

        expected_size = remote_file.get("size_bytes")

        if not isinstance(expected_size, int):
            raise ValueError(
                "La taille attendue du fichier est absente "
                "ou invalide dans le manifeste."
            )

        downloader = LaDeFileDownloader(
            repository_id=repository_id,
            revision=revision,
            destination_root=settings.resolved_lade_data_dir,
        )

        downloaded_file = downloader.download(
            remote_path=arguments.remote_path,
            expected_size_bytes=expected_size,
            force_download=arguments.force,
        )

        download_manifest_path = (
            build_download_manifest_path(
                metadata_directory=(
                    settings.resolved_lade_data_dir
                    / "_metadata"
                    / "downloads"
                ),
                remote_path=arguments.remote_path,
            )
        )

        downloader.write_download_manifest(
            downloaded_file=downloaded_file,
            output_path=download_manifest_path,
        )

    except (
        FileNotFoundError,
        ValueError,
        LaDeDownloadError,
    ) as error:
        print()
        print(f"Erreur : {error}")
        return 1

    print_download_summary(
        repository_id=downloaded_file.repository_id,
        revision=downloaded_file.revision,
        remote_path=downloaded_file.remote_path,
        local_path=downloaded_file.local_path,
        size_bytes=downloaded_file.size_bytes,
        size_megabytes=downloaded_file.size_megabytes,
        sha256=downloaded_file.sha256,
        manifest_path=download_manifest_path,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())