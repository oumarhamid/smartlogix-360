from __future__ import annotations

import argparse
import json
from json import JSONDecodeError
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from smartlogix.common import configure_logging
from smartlogix.config import get_settings
from smartlogix.ingestion.lade import (
    LaDeBronzeBuilder,
    LaDeBronzeBuildError,
    LaDeDeliveryQualityValidator,
    read_lade_delivery_csv,
)

DEFAULT_RELATIVE_INPUT = (
    Path("delivery")
    / "delivery_jl.csv"
)

DEFAULT_DOWNLOAD_MANIFEST = (
    Path("_metadata")
    / "downloads"
    / "delivery__delivery_jl.csv.download.json"
)


def parse_arguments() -> argparse.Namespace:
    """Analyse les arguments de la ligne de commande."""

    parser = argparse.ArgumentParser(
        description=(
            "Valide un fichier LaDe-D et produit sa "
            "représentation Bronze en Parquet."
        )
    )

    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help=(
            "Chemin du CSV source. Par défaut, utilise "
            "delivery/delivery_jl.csv."
        ),
    )

    parser.add_argument(
        "--download-manifest",
        type=Path,
        default=None,
        help=(
            "Chemin du manifeste contenant le SHA-256 "
            "et la révision du dataset."
        ),
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Chemin facultatif du Parquet Bronze.",
    )

    parser.add_argument(
        "--build-manifest",
        type=Path,
        default=None,
        help=(
            "Chemin facultatif du manifeste de production "
            "Bronze."
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
            "none",
        ],
        help="Compression du fichier Parquet.",
    )

    return parser.parse_args()


def load_json_object(
    file_path: Path,
) -> dict[str, Any]:
    """Charge un objet JSON depuis un fichier."""

    resolved_path = file_path.resolve()

    if not resolved_path.is_file():
        raise FileNotFoundError(
            f"Le fichier JSON est introuvable : {resolved_path}"
        )

    try:
        content = json.loads(
            resolved_path.read_text(
                encoding="utf-8"
            )
        )
    except JSONDecodeError as error:
        raise ValueError(
            f"Le fichier JSON est invalide : {resolved_path}"
        ) from error

    if not isinstance(content, dict):
        raise ValueError(
            "Le contenu JSON doit être un objet."
        )

    return content


def extract_source_metadata(
    download_manifest: dict[str, Any],
) -> tuple[str, str]:
    """Extrait le SHA-256 et la révision du dataset."""

    source_sha256 = download_manifest.get("sha256")
    dataset_revision = download_manifest.get("revision")

    if (
        not isinstance(source_sha256, str)
        or not source_sha256
    ):
        raise ValueError(
            "Le SHA-256 est absent ou invalide dans "
            "le manifeste de téléchargement."
        )

    if (
        not isinstance(dataset_revision, str)
        or not dataset_revision
    ):
        raise ValueError(
            "La révision est absente ou invalide dans "
            "le manifeste de téléchargement."
        )

    return source_sha256, dataset_revision


def find_project_data_root(
    raw_lade_directory: Path,
) -> Path:
    """Retrouve le dossier data à partir de data/raw/lade."""

    resolved_directory = raw_lade_directory.resolve()

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
    """Construit le chemin Parquet Bronze par défaut."""

    return (
        project_data_root
        / "bronze"
        / "lade"
        / "delivery"
        / f"{source_path.stem}.parquet"
    )


def build_default_manifest_path(
    project_data_root: Path,
    output_path: Path,
) -> Path:
    """Construit le chemin du manifeste Bronze."""

    normalized_name = (
        output_path.name
        .replace("/", "__")
        .replace("\\", "__")
    )

    return (
        project_data_root
        / "bronze"
        / "lade"
        / "_metadata"
        / "builds"
        / f"{normalized_name}.build.json"
    )


def write_build_manifest(
    content: dict[str, Any],
    output_path: Path,
) -> Path:
    """Écrit atomiquement le manifeste Bronze."""

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


def read_parquet_metadata(
    parquet_path: Path,
) -> tuple[int, int, int]:
    """Lit les métadonnées du Parquet sans charger ses données."""

    with parquet_path.open("rb") as parquet_stream:
        parquet_file = pq.ParquetFile(
            parquet_stream
        )

        row_count = int(
            parquet_file.metadata.num_rows
        )

        column_count = int(
            parquet_file.metadata.num_columns
        )

        row_group_count = int(
            parquet_file.metadata.num_row_groups
        )

    return (
        row_count,
        column_count,
        row_group_count,
    )


def print_summary(
    source_path: Path,
    output_path: Path,
    build_manifest_path: Path,
    source_row_count: int,
    parquet_row_count: int,
    parquet_column_count: int,
    row_group_count: int,
    valid_row_count: int,
    warning_row_count: int,
    source_size_bytes: int,
    parquet_size_bytes: int,
    compression: str,
    dataset_revision: str,
) -> None:
    """Affiche le résumé de la production Bronze."""

    compression_ratio = (
        round(
            parquet_size_bytes
            / source_size_bytes,
            4,
        )
        if source_size_bytes
        else None
    )

    print()
    print("=" * 74)
    print("SMARTLOGIX 360 - PRODUCTION BRONZE LADE-D")
    print("=" * 74)
    print(f"Source CSV          : {source_path}")
    print(f"Parquet Bronze      : {output_path}")
    print(f"Révision dataset    : {dataset_revision}")
    print(f"Compression         : {compression}")
    print(f"Lignes source       : {source_row_count}")
    print(f"Lignes Parquet      : {parquet_row_count}")
    print(f"Colonnes Parquet    : {parquet_column_count}")
    print(f"Groupes de lignes   : {row_group_count}")
    print(f"Lignes valides      : {valid_row_count}")
    print(f"Lignes avertissement: {warning_row_count}")
    print(f"Taille CSV          : {source_size_bytes} octets")
    print(f"Taille Parquet      : {parquet_size_bytes} octets")
    print(f"Ratio Parquet/CSV   : {compression_ratio}")
    print(f"Manifeste Bronze    : {build_manifest_path}")
    print("=" * 74)


def main() -> int:
    """Produit le fichier Bronze à partir du CSV LaDe-D."""

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
            raw_lade_directory
            / DEFAULT_RELATIVE_INPUT
        )
    ).resolve()

    download_manifest_path = (
        arguments.download_manifest
        if arguments.download_manifest is not None
        else (
            raw_lade_directory
            / DEFAULT_DOWNLOAD_MANIFEST
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

    build_manifest_path = (
        arguments.build_manifest
        if arguments.build_manifest is not None
        else build_default_manifest_path(
            project_data_root=project_data_root,
            output_path=output_path,
        )
    ).resolve()

    compression = (
        None
        if arguments.compression == "none"
        else arguments.compression
    )

    try:
        download_manifest = load_json_object(
            download_manifest_path
        )

        source_sha256, dataset_revision = (
            extract_source_metadata(
                download_manifest
            )
        )

        dataframe = read_lade_delivery_csv(
            source_path
        )

        quality_report = (
            LaDeDeliveryQualityValidator()
            .validate(
                dataframe=dataframe,
                source_path=source_path,
            )
        )

        if not quality_report.blocking_passed:
            print()
            print(
                "Erreur : le contrat Pandera contient "
                f"{quality_report.blocking_failure_count} "
                "erreur(s) bloquante(s)."
            )
            return 2

        builder = LaDeBronzeBuilder(
            compression=(
                compression or "none"
            )
        )

        result = builder.build(
            dataframe=dataframe,
            quality_report=quality_report,
            source_path=source_path,
            output_path=output_path,
            source_sha256=source_sha256,
            dataset_revision=dataset_revision,
        )

        (
            parquet_row_count,
            parquet_column_count,
            row_group_count,
        ) = read_parquet_metadata(output_path)

        if parquet_row_count != len(dataframe):
            raise LaDeBronzeBuildError(
                "Le nombre de lignes Parquet ne correspond "
                "pas au nombre de lignes du CSV source."
            )

        manifest_content = result.to_dict()

        manifest_content.update(
            {
                "source_size_bytes": (
                    source_path.stat().st_size
                ),
                "parquet_row_count": (
                    parquet_row_count
                ),
                "parquet_column_count": (
                    parquet_column_count
                ),
                "parquet_row_group_count": (
                    row_group_count
                ),
                "quality_warning_rule_count": (
                    quality_report.warning_rule_count
                ),
                "quality_warning_row_count": (
                    quality_report.warning_row_count
                ),
                "blocking_contract_passed": (
                    quality_report.blocking_passed
                ),
            }
        )

        written_manifest_path = (
            write_build_manifest(
                content=manifest_content,
                output_path=build_manifest_path,
            )
        )

    except (
        FileNotFoundError,
        ValueError,
        OSError,
        LaDeBronzeBuildError,
    ) as error:
        print()
        print(f"Erreur : {error}")
        return 1

    print_summary(
        source_path=source_path,
        output_path=output_path,
        build_manifest_path=(
            written_manifest_path
        ),
        source_row_count=len(dataframe),
        parquet_row_count=parquet_row_count,
        parquet_column_count=(
            parquet_column_count
        ),
        row_group_count=row_group_count,
        valid_row_count=result.valid_row_count,
        warning_row_count=(
            result.warning_row_count
        ),
        source_size_bytes=(
            source_path.stat().st_size
        ),
        parquet_size_bytes=(
            result.parquet_size_bytes
        ),
        compression=arguments.compression,
        dataset_revision=dataset_revision,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())