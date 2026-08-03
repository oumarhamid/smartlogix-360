from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from smartlogix.common import configure_logging
from smartlogix.config import get_settings
from smartlogix.ingestion.lade import (
    LaDeDeliveryQualityValidator,
    LaDeQualityReport,
    read_lade_delivery_csv,
)

DEFAULT_RELATIVE_INPUT = (
    Path("delivery")
    / "delivery_jl.csv"
)


def parse_arguments() -> argparse.Namespace:
    """Analyse les arguments de la ligne de commande."""

    parser = argparse.ArgumentParser(
        description=(
            "Valide un fichier LaDe-D avec le contrat Pandera "
            "et génère un rapport de qualité JSON."
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

    parser.add_argument(
        "--sample-limit",
        type=int,
        default=20,
        help=(
            "Nombre maximal d'exemples conservés "
            "pour chaque anomalie."
        ),
    )

    parser.add_argument(
        "--long-duration-minutes",
        type=float,
        default=24 * 60,
        help=(
            "Seuil en minutes au-delà duquel une durée "
            "de livraison génère un avertissement."
        ),
    )

    parser.add_argument(
        "--near-zero-coordinate",
        type=float,
        default=1.0,
        help=(
            "Valeur absolue sous laquelle une coordonnée "
            "GPS est considérée comme proche de zéro."
        ),
    )

    arguments = parser.parse_args()

    if arguments.sample_limit <= 0:
        parser.error(
            "--sample-limit doit être strictement positif."
        )

    if arguments.long_duration_minutes <= 0:
        parser.error(
            "--long-duration-minutes doit être "
            "strictement positif."
        )

    if arguments.near_zero_coordinate <= 0:
        parser.error(
            "--near-zero-coordinate doit être "
            "strictement positif."
        )

    return arguments


def build_default_report_path(
    data_root: Path,
    csv_path: Path,
) -> Path:
    """Construit le chemin du rapport de qualité."""

    resolved_data_root = data_root.resolve()
    resolved_csv_path = csv_path.resolve()

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
        resolved_data_root
        / "_metadata"
        / "quality"
        / f"{normalized_name}.quality.json"
    )


def print_blocking_summary(
    report: LaDeQualityReport,
) -> None:
    """Affiche le résultat des règles bloquantes."""

    print("\nContrat bloquant Pandera :")

    if report.blocking_passed:
        print("  - Statut                 : VALIDÉ")
        print("  - Erreurs bloquantes    : 0")
        return

    print("  - Statut                 : ÉCHEC")
    print(
        "  - Erreurs bloquantes    : "
        f"{report.blocking_failure_count}"
    )

    if not report.blocking_failure_cases:
        return

    print("\n  Exemples d'erreurs :")

    for failure in report.blocking_failure_cases:
        column = failure.get("column", "inconnue")
        check = failure.get("check", "inconnu")
        index = failure.get("index", "inconnu")
        value = failure.get("failure_case", "inconnue")

        print(
            f"    - colonne={column}, "
            f"règle={check}, "
            f"ligne={index}, "
            f"valeur={value}"
        )


def print_warning_summary(
    report: LaDeQualityReport,
) -> None:
    """Affiche les avertissements métier."""

    print("\nAvertissements métier :")
    print(
        "  - Règles déclenchées    : "
        f"{report.warning_rule_count}"
    )
    print(
        "  - Lignes concernées     : "
        f"{report.warning_row_count}"
    )

    if not report.warnings:
        print("  - Aucun avertissement détecté")
        return

    for warning in report.warnings:
        print()
        print(f"  - Règle       : {warning.rule}")
        print(
            f"    Lignes      : {warning.row_count}"
        )
        print(
            f"    Colonnes    : "
            f"{', '.join(warning.columns)}"
        )
        print(
            f"    Description : {warning.description}"
        )


def print_quality_summary(
    report: LaDeQualityReport,
    report_path: Path,
) -> None:
    """Affiche le résumé complet de la validation."""

    print()
    print("=" * 74)
    print("SMARTLOGIX 360 - VALIDATION QUALITÉ LADE-D")
    print("=" * 74)
    print(f"Fichier source     : {report.source_path}")
    print(f"Enregistrements    : {report.row_count}")

    print_blocking_summary(report)
    print_warning_summary(report)

    print(f"\nRapport JSON       : {report_path}")
    print("=" * 74)


def main() -> int:
    """Exécute la validation réelle du fichier LaDe-D."""

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
            data_root=data_root,
            csv_path=csv_path,
        )
    )

    validator = LaDeDeliveryQualityValidator(
        sample_limit=arguments.sample_limit,
        long_duration_minutes=(
            arguments.long_duration_minutes
        ),
        near_zero_coordinate=(
            arguments.near_zero_coordinate
        ),
    )

    try:
        dataframe = read_lade_delivery_csv(csv_path)

        report = validator.validate(
            dataframe=dataframe,
            source_path=csv_path,
        )

        validator.write_report(
            report=report,
            output_path=report_path,
        )

    except (
        FileNotFoundError,
        OSError,
        UnicodeDecodeError,
        pd.errors.ParserError,
    ) as error:
        print()
        print(f"Erreur : {error}")
        return 1

    print_quality_summary(
        report=report,
        report_path=report_path,
    )

    if not report.blocking_passed:
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())