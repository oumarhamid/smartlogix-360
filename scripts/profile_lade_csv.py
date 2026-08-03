from __future__ import annotations

import argparse
from pathlib import Path

from smartlogix.common import configure_logging
from smartlogix.config import get_settings
from smartlogix.ingestion.lade import (
    LaDeCsvProfile,
    LaDeCsvProfiler,
    LaDeCsvProfilingError,
)

DEFAULT_RELATIVE_INPUT = (
    Path("delivery")
    / "delivery_jl.csv"
)


def parse_arguments() -> argparse.Namespace:
    """Analyse les arguments de la ligne de commande."""

    parser = argparse.ArgumentParser(
        description=(
            "Profile un fichier CSV LaDe et génère un rapport "
            "JSON de schéma et de qualité."
        )
    )

    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help=(
            "Chemin du fichier CSV. Par défaut, utilise "
            "delivery/delivery_jl.csv dans le dossier LaDe."
        ),
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Chemin facultatif du rapport JSON généré.",
    )

    return parser.parse_args()


def build_default_report_path(
    metadata_directory: Path,
    csv_path: Path,
    data_root: Path,
) -> Path:
    """Construit le chemin du rapport de profilage."""

    resolved_csv_path = csv_path.resolve()
    resolved_data_root = data_root.resolve()

    try:
        relative_path = resolved_csv_path.relative_to(
            resolved_data_root
        )
    except ValueError:
        relative_path = Path(resolved_csv_path.name)

    normalized_name = (
        relative_path.as_posix()
        .replace("/", "__")
    )

    return (
        metadata_directory
        / "profiles"
        / f"{normalized_name}.profile.json"
    )


def print_schema_summary(
    profile: LaDeCsvProfile,
) -> None:
    """Affiche les différences entre le schéma attendu et réel."""

    print("\nSchéma :")

    if not profile.missing_columns:
        print("  - Colonnes manquantes    : aucune")
    else:
        print(
            "  - Colonnes manquantes    : "
            + ", ".join(profile.missing_columns)
        )

    if not profile.unexpected_columns:
        print("  - Colonnes inattendues   : aucune")
    else:
        print(
            "  - Colonnes inattendues   : "
            + ", ".join(profile.unexpected_columns)
        )


def print_null_summary(
    profile: LaDeCsvProfile,
) -> None:
    """Affiche les colonnes contenant des valeurs manquantes."""

    columns_with_nulls = [
        column
        for column in profile.columns
        if column.null_count > 0
    ]

    print("\nValeurs manquantes :")

    if not columns_with_nulls:
        print("  - Aucune valeur manquante")
        return

    columns_with_nulls.sort(
        key=lambda column: column.null_count,
        reverse=True,
    )

    for column in columns_with_nulls:
        print(
            f"  - {column.name:<22} "
            f"{column.null_count:>7} "
            f"({column.null_percentage:>7.3f} %)"
        )


def print_coordinate_summary(
    profile: LaDeCsvProfile,
) -> None:
    """Affiche les bornes des coordonnées géographiques."""

    print("\nBornes géographiques :")

    if not profile.coordinate_bounds:
        print("  - Aucune coordonnée disponible")
        return

    for column_name, bounds in (
        profile.coordinate_bounds.items()
    ):
        print(
            f"  - {column_name:<22} "
            f"min={bounds['minimum']} "
            f"max={bounds['maximum']}"
        )


def print_duration_summary(
    profile: LaDeCsvProfile,
) -> None:
    """Affiche les statistiques de durée de livraison."""

    durations = profile.delivery_duration_minutes

    print("\nDurées de livraison en minutes :")
    print(
        "  - Durées valides          : "
        f"{durations['valid_count']}"
    )
    print(
        "  - Durées invalides        : "
        f"{durations['missing_or_invalid_count']}"
    )
    print(
        "  - Minimum                 : "
        f"{durations['minimum']}"
    )
    print(
        "  - Maximum                 : "
        f"{durations['maximum']}"
    )
    print(
        "  - Moyenne                 : "
        f"{durations['mean']}"
    )
    print(
        "  - Médiane                 : "
        f"{durations['median']}"
    )
    print(
        "  - Passages après minuit   : "
        f"{durations['midnight_rollover_count']}"
    )


def print_profile_summary(
    profile: LaDeCsvProfile,
    report_path: Path,
) -> None:
    """Affiche le résumé complet du profilage."""

    print()
    print("=" * 72)
    print("SMARTLOGIX 360 - PROFILAGE CSV LADE")
    print("=" * 72)
    print(f"Fichier            : {profile.file_path}")
    print(f"Taille             : {profile.file_size_bytes} octets")
    print(f"Enregistrements    : {profile.row_count}")
    print(f"Colonnes           : {profile.column_count}")
    print(
        "Lignes dupliquées  : "
        f"{profile.duplicate_row_count}"
    )

    print_schema_summary(profile)
    print_null_summary(profile)
    print_coordinate_summary(profile)
    print_duration_summary(profile)

    print(f"\nRapport JSON       : {report_path}")
    print("=" * 72)


def main() -> int:
    """Profile le fichier CSV LaDe sélectionné."""

    configure_logging()

    arguments = parse_arguments()
    settings = get_settings()

    data_root = settings.resolved_lade_data_dir

    csv_path = (
        arguments.input
        if arguments.input is not None
        else data_root / DEFAULT_RELATIVE_INPUT
    )

    report_path = (
        arguments.output
        if arguments.output is not None
        else build_default_report_path(
            metadata_directory=(
                data_root / "_metadata"
            ),
            csv_path=csv_path,
            data_root=data_root,
        )
    )

    profiler = LaDeCsvProfiler()

    try:
        profile = profiler.profile(csv_path)

        profiler.write_report(
            profile=profile,
            output_path=report_path,
        )
    except LaDeCsvProfilingError as error:
        print()
        print(f"Erreur : {error}")
        return 1

    print_profile_summary(
        profile=profile,
        report_path=report_path,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())