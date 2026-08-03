from __future__ import annotations

import argparse
from pathlib import Path

from smartlogix.common import configure_logging
from smartlogix.config import get_settings
from smartlogix.ingestion.lade import (
    LaDeRepositoryInspectionError,
    LaDeRepositoryInspector,
)


def parse_arguments() -> argparse.Namespace:
    """Analyse les arguments de la ligne de commande."""

    parser = argparse.ArgumentParser(
        description=(
            "Inspecte le dépôt public LaDe sur Hugging Face et "
            "enregistre son inventaire sans télécharger les datasets."
        )
    )

    parser.add_argument(
        "--repository-id",
        default=None,
        help=(
            "Identifiant du dépôt Hugging Face. "
            "Par défaut, utilise la configuration SmartLogix."
        ),
    )

    parser.add_argument(
        "--revision",
        default="main",
        help="Branche, tag ou commit du dépôt à inspecter.",
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Chemin facultatif du manifeste JSON généré.",
    )

    return parser.parse_args()


def print_inventory_summary(
    repository_id: str,
    revision: str,
    commit_sha: str,
    file_count: int,
    total_size_gigabytes: float,
    unknown_size_count: int,
    category_counts: dict[str, int],
    extension_counts: dict[str, int],
    output_path: Path,
) -> None:
    """Affiche un résumé lisible de l'inventaire."""

    print()
    print("=" * 65)
    print("SMARTLOGIX 360 - INVENTAIRE DISTANT LADE")
    print("=" * 65)
    print(f"Dépôt                : {repository_id}")
    print(f"Révision demandée    : {revision}")
    print(f"Commit distant       : {commit_sha}")
    print(f"Nombre de fichiers   : {file_count}")
    print(f"Taille totale connue : {total_size_gigabytes} Go")
    print(f"Tailles inconnues    : {unknown_size_count}")

    print("\nRépartition par catégorie :")

    for category, count in category_counts.items():
        print(f"  - {category:<22} {count}")

    print("\nRépartition par extension :")

    for extension, count in extension_counts.items():
        print(f"  - {extension:<22} {count}")

    print(f"\nManifeste local : {output_path}")
    print("=" * 65)


def main() -> int:
    """Exécute l'inspection distante du dépôt LaDe."""

    configure_logging()

    arguments = parse_arguments()
    settings = get_settings()

    repository_id = (
        arguments.repository_id
        or settings.lade_dataset_repository
    )

    output_path = arguments.output or (
        settings.resolved_lade_data_dir
        / "_metadata"
        / "remote_manifest.json"
    )

    inspector = LaDeRepositoryInspector(
        repository_id=repository_id,
        revision=arguments.revision,
    )

    try:
        inventory = inspector.inspect()

        inspector.write_manifest(
            inventory=inventory,
            output_path=output_path,
        )
    except LaDeRepositoryInspectionError as error:
        print()
        print(f"Erreur : {error}")
        print(
            "Vérifie la connexion Internet, le nom du dépôt "
            "et la disponibilité de Hugging Face."
        )
        return 1

    print_inventory_summary(
        repository_id=inventory.repository_id,
        revision=inventory.revision,
        commit_sha=inventory.commit_sha,
        file_count=inventory.file_count,
        total_size_gigabytes=(
            inventory.known_total_size_gigabytes
        ),
        unknown_size_count=(
            inventory.files_with_unknown_size_count
        ),
        category_counts=inventory.category_counts(),
        extension_counts=inventory.extension_counts(),
        output_path=output_path,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())