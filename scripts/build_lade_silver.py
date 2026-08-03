from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from smartlogix.common import configure_logging
from smartlogix.config import get_settings
from smartlogix.ingestion.lade import (
    LaDeSilverBuilder,
    LaDeSilverBuildError,
)

DEFAULT_RELATIVE_BRONZE_INPUT = (
    Path("lade")
    / "delivery"
    / "delivery_jl.parquet"
)


def parse_arguments() -> argparse.Namespace:
    """Analyse les arguments de la ligne de commande."""

    parser = argparse.ArgumentParser(
        description=(
            "Transforme un fichier Bronze LaDe-D "
            "en fichier Silver nettoyé et enrichi."
        )
    )

    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help=(
            "Chemin du Parquet Bronze. Par défaut, utilise "
            "data/bronze/lade/delivery/delivery_jl.parquet."
        ),
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Chemin facultatif du Parquet Silver.",
    )

    parser.add_argument(
        "--build-manifest",
        type=Path,
        default=None,
        help=(
            "Chemin facultatif du manifeste JSON "
            "de production Silver."
        ),
    )

    parser.add_argument(
        "--compression",
        default="zstd",
        choices=[
            "zstd",
            "snappy",
            "gzip",
            "brotli",
            "lz4",
        ],
        help="Algorithme de compression du Parquet Silver.",
    )

    parser.add_argument(
        "--near-zero-coordinate",
        type=float,
        default=1.0,
        help=(
            "Seuil absolu sous lequel une coordonnée GPS "
            "est considérée comme sentinelle ou invalide."
        ),
    )

    parser.add_argument(
        "--long-duration-minutes",
        type=float,
        default=24 * 60,
        help=(
            "Seuil en minutes au-delà duquel une livraison "
            "est classée comme anormalement longue."
        ),
    )

    arguments = parser.parse_args()

    if arguments.near_zero_coordinate <= 0:
        parser.error(
            "--near-zero-coordinate doit être "
            "strictement positif."
        )

    if arguments.long_duration_minutes <= 0:
        parser.error(
            "--long-duration-minutes doit être "
            "strictement positif."
        )

    return arguments


def find_project_data_root(
    raw_lade_directory: Path,
) -> Path:
    """Retrouve le dossier data depuis data/raw/lade."""

    resolved_directory = (
        raw_lade_directory.resolve()
    )

    if (
        resolved_directory.name == "lade"
        and resolved_directory.parent.name == "raw"
    ):
        return resolved_directory.parent.parent

    return (
        Path.cwd()
        / "data"
    ).resolve()


def build_default_output_path(
    project_data_root: Path,
    source_path: Path,
) -> Path:
    """Construit le chemin Silver par défaut."""

    return (
        project_data_root
        / "silver"
        / "lade"
        / "delivery"
        / source_path.name
    )


def build_default_manifest_path(
    project_data_root: Path,
    output_path: Path,
) -> Path:
    """Construit le chemin du manifeste Silver."""

    return (
        project_data_root
        / "silver"
        / "lade"
        / "_metadata"
        / "builds"
        / f"{output_path.name}.build.json"
    )


def load_bronze_parquet(
    source_path: Path,
) -> pd.DataFrame:
    """Charge le fichier Parquet Bronze."""

    resolved_path = source_path.resolve()

    if not resolved_path.is_file():
        raise FileNotFoundError(
            "Le fichier Bronze est introuvable : "
            f"{resolved_path}"
        )

    try:
        return pd.read_parquet(
            resolved_path,
            engine="pyarrow",
        )
    except Exception as error:
        raise LaDeSilverBuildError(
            "Impossible de lire le fichier Bronze : "
            f"{resolved_path}"
        ) from error


def read_parquet_metadata(
    parquet_path: Path,
) -> dict[str, Any]:
    """Lit les métadonnées d'un Parquet sans charger ses lignes."""

    resolved_path = parquet_path.resolve()

    with resolved_path.open("rb") as parquet_stream:
        parquet_file = pq.ParquetFile(
            parquet_stream
        )

        metadata = parquet_file.metadata

        return {
            "row_count": int(metadata.num_rows),
            "column_count": int(metadata.num_columns),
            "row_group_count": int(
                metadata.num_row_groups
            ),
            "columns": list(
                parquet_file.schema_arrow.names
            ),
        }


def write_build_manifest(
    content: dict[str, Any],
    output_path: Path,
) -> Path:
    """Écrit atomiquement le manifeste Silver."""

    resolved_path = output_path.resolve()

    resolved_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_path = resolved_path.with_name(
        f"{resolved_path.name}.tmp"
    )

    temporary_path.write_text(
        json.dumps(
            content,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    temporary_path.replace(resolved_path)

    return resolved_path


def print_summary(
    source_path: Path,
    output_path: Path,
    manifest_path: Path,
    source_row_count: int,
    silver_row_count: int,
    silver_column_count: int,
    row_group_count: int,
    quality_warning_row_count: int,
    gps_issue_row_count: int,
    duration_issue_row_count: int,
    source_size_bytes: int,
    silver_size_bytes: int,
    compression: str,
    silver_version: str,
) -> None:
    """Affiche le résumé de la production Silver."""

    size_ratio = (
        round(
            silver_size_bytes
            / source_size_bytes,
            4,
        )
        if source_size_bytes
        else None
    )

    print()
    print("=" * 76)
    print("SMARTLOGIX 360 - PRODUCTION SILVER LADE-D")
    print("=" * 76)
    print(f"Source Bronze       : {source_path}")
    print(f"Parquet Silver      : {output_path}")
    print(f"Version Silver      : {silver_version}")
    print(f"Compression         : {compression}")
    print(f"Lignes Bronze       : {source_row_count}")
    print(f"Lignes Silver       : {silver_row_count}")
    print(f"Colonnes Silver     : {silver_column_count}")
    print(f"Groupes de lignes   : {row_group_count}")
    print(
        "Lignes qualité warn : "
        f"{quality_warning_row_count}"
    )
    print(
        "Lignes problème GPS : "
        f"{gps_issue_row_count}"
    )
    print(
        "Lignes durée anormale: "
        f"{duration_issue_row_count}"
    )
    print(f"Taille Bronze       : {source_size_bytes} octets")
    print(f"Taille Silver       : {silver_size_bytes} octets")
    print(f"Ratio Silver/Bronze : {size_ratio}")
    print(f"Manifeste Silver    : {manifest_path}")
    print("=" * 76)


def main() -> int:
    """Produit le fichier Silver depuis le Parquet Bronze."""

    configure_logging()

    arguments = parse_arguments()
    settings = get_settings()

    raw_lade_directory = (
        settings.resolved_lade_data_dir
    )

    project_data_root = find_project_data_root(
        raw_lade_directory
    )

    source_path = (
        arguments.input
        if arguments.input is not None
        else (
            project_data_root
            / "bronze"
            / DEFAULT_RELATIVE_BRONZE_INPUT
        )
    ).resolve()

    output_path = (
        arguments.output
        if arguments.output is not None
        else build_default_output_path(
            project_data_root=project_data_root,
            source_path=source_path,
        )
    ).resolve()

    manifest_path = (
        arguments.build_manifest
        if arguments.build_manifest is not None
        else build_default_manifest_path(
            project_data_root=project_data_root,
            output_path=output_path,
        )
    ).resolve()

    try:
        bronze_dataframe = load_bronze_parquet(
            source_path
        )

        builder = LaDeSilverBuilder(
            compression=arguments.compression,
            near_zero_coordinate=(
                arguments.near_zero_coordinate
            ),
            long_duration_minutes=(
                arguments.long_duration_minutes
            ),
        )

        result = builder.build(
            dataframe=bronze_dataframe,
            source_path=source_path,
            output_path=output_path,
        )

        silver_metadata = read_parquet_metadata(
            output_path
        )

        silver_row_count = int(
            silver_metadata["row_count"]
        )

        if silver_row_count != len(
            bronze_dataframe
        ):
            raise LaDeSilverBuildError(
                "Le nombre de lignes Silver ne correspond "
                "pas au nombre de lignes Bronze."
            )

        source_size_bytes = (
            source_path.stat().st_size
        )

        manifest_content = result.to_dict()

        manifest_content.update(
            {
                "source_size_bytes": (
                    source_size_bytes
                ),
                "silver_row_count": (
                    silver_row_count
                ),
                "silver_column_count": (
                    silver_metadata["column_count"]
                ),
                "silver_row_group_count": (
                    silver_metadata["row_group_count"]
                ),
                "silver_columns": (
                    silver_metadata["columns"]
                ),
                "near_zero_coordinate": (
                    arguments.near_zero_coordinate
                ),
                "long_duration_minutes": (
                    arguments.long_duration_minutes
                ),
            }
        )

        written_manifest_path = (
            write_build_manifest(
                content=manifest_content,
                output_path=manifest_path,
            )
        )

    except (
        FileNotFoundError,
        ValueError,
        OSError,
        LaDeSilverBuildError,
    ) as error:
        print()
        print(f"Erreur : {error}")
        return 1

    print_summary(
        source_path=source_path,
        output_path=output_path,
        manifest_path=written_manifest_path,
        source_row_count=len(bronze_dataframe),
        silver_row_count=silver_row_count,
        silver_column_count=int(
            silver_metadata["column_count"]
        ),
        row_group_count=int(
            silver_metadata["row_group_count"]
        ),
        quality_warning_row_count=(
            result.quality_warning_row_count
        ),
        gps_issue_row_count=(
            result.gps_issue_row_count
        ),
        duration_issue_row_count=(
            result.duration_issue_row_count
        ),
        source_size_bytes=source_size_bytes,
        silver_size_bytes=(
            result.parquet_size_bytes
        ),
        compression=result.compression,
        silver_version=result.silver_version,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())