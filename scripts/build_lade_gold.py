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
    LaDeGoldBuilder,
    LaDeGoldBuildError,
)

DEFAULT_RELATIVE_SILVER_INPUT = (
    Path("lade")
    / "delivery"
    / "delivery_jl.parquet"
)


def parse_arguments() -> argparse.Namespace:
    """Analyse les arguments de la ligne de commande."""

    parser = argparse.ArgumentParser(
        description=(
            "Transforme un fichier Silver LaDe-D "
            "en trois tables analytiques Gold."
        )
    )

    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help=(
            "Chemin du Parquet Silver. Par défaut : "
            "data/silver/lade/delivery/delivery_jl.parquet."
        ),
    )

    parser.add_argument(
        "--output-directory",
        type=Path,
        default=None,
        help=(
            "Dossier de sortie des trois tables Gold. "
            "Par défaut : data/gold/lade/delivery."
        ),
    )

    parser.add_argument(
        "--build-manifest",
        type=Path,
        default=None,
        help=(
            "Chemin facultatif du manifeste JSON "
            "de production Gold."
        ),
    )

    parser.add_argument(
        "--sla-minutes",
        type=float,
        default=240.0,
        help=(
            "Durée maximale en minutes considérée "
            "comme respectant le SLA."
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
        help="Algorithme de compression des fichiers Parquet.",
    )

    arguments = parser.parse_args()

    if arguments.sla_minutes <= 0:
        parser.error(
            "--sla-minutes doit être strictement positif."
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


def build_default_output_directory(
    project_data_root: Path,
) -> Path:
    """Construit le dossier Gold par défaut."""

    return (
        project_data_root
        / "gold"
        / "lade"
        / "delivery"
    )


def build_default_manifest_path(
    project_data_root: Path,
) -> Path:
    """Construit le chemin du manifeste Gold."""

    return (
        project_data_root
        / "gold"
        / "lade"
        / "_metadata"
        / "builds"
        / "delivery_gold.build.json"
    )


def load_silver_parquet(
    source_path: Path,
) -> pd.DataFrame:
    """Charge le fichier Parquet Silver."""

    resolved_path = source_path.resolve()

    if not resolved_path.is_file():
        raise FileNotFoundError(
            "Le fichier Silver est introuvable : "
            f"{resolved_path}"
        )

    try:
        return pd.read_parquet(
            resolved_path,
            engine="pyarrow",
        )
    except Exception as error:
        raise LaDeGoldBuildError(
            "Impossible de lire le fichier Silver : "
            f"{resolved_path}"
        ) from error


def read_parquet_metadata(
    parquet_path: Path,
) -> dict[str, Any]:
    """Lit les métadonnées d'un fichier Parquet."""

    resolved_path = parquet_path.resolve()

    if not resolved_path.is_file():
        raise FileNotFoundError(
            "Le fichier Parquet Gold est introuvable : "
            f"{resolved_path}"
        )

    with resolved_path.open("rb") as parquet_stream:
        parquet_file = pq.ParquetFile(
            parquet_stream
        )

        metadata = parquet_file.metadata

        return {
            "path": str(resolved_path),
            "row_count": int(metadata.num_rows),
            "column_count": int(metadata.num_columns),
            "row_group_count": int(
                metadata.num_row_groups
            ),
            "size_bytes": int(
                resolved_path.stat().st_size
            ),
            "columns": list(
                parquet_file.schema_arrow.names
            ),
        }


def calculate_fact_kpis(
    delivery_fact_path: Path,
) -> dict[str, int]:
    """Calcule les principaux KPI depuis la table de faits."""

    columns = [
        "delivery_count",
        "is_valid_duration",
        "is_within_sla",
        "is_late_delivery",
        "has_complete_gps",
        "is_quality_warning",
    ]

    dataframe = pd.read_parquet(
        delivery_fact_path,
        engine="pyarrow",
        columns=columns,
    )

    return {
        "delivery_count": int(
            dataframe["delivery_count"].sum()
        ),
        "valid_duration_count": int(
            dataframe["is_valid_duration"].sum()
        ),
        "within_sla_count": int(
            dataframe["is_within_sla"].sum()
        ),
        "late_delivery_count": int(
            dataframe["is_late_delivery"].sum()
        ),
        "complete_gps_count": int(
            dataframe["has_complete_gps"].sum()
        ),
        "quality_warning_count": int(
            dataframe["is_quality_warning"].sum()
        ),
    }


def calculate_aggregate_order_total(
    parquet_path: Path,
) -> int:
    """Additionne la colonne orders_total d'un agrégat Gold."""

    dataframe = pd.read_parquet(
        parquet_path,
        engine="pyarrow",
        columns=["orders_total"],
    )

    return int(
        dataframe["orders_total"].sum()
    )


def calculate_courier_eligible_row_count(
    parquet_path: Path,
) -> int:
    """Compte les livraisons attribuées à un coursier réel."""

    dataframe = pd.read_parquet(
        parquet_path,
        engine="pyarrow",
        columns=["courier_id"],
    )

    return int(
        dataframe["courier_id"].gt(0).sum()
    )


def write_build_manifest(
    content: dict[str, Any],
    output_path: Path,
) -> Path:
    """Écrit atomiquement le manifeste Gold."""

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


def print_table_summary(
    table_name: str,
    metadata: dict[str, Any],
) -> None:
    """Affiche le résumé d'une table Gold."""

    print()
    print(f"Table                 : {table_name}")
    print(f"Chemin                : {metadata['path']}")
    print(f"Lignes                : {metadata['row_count']}")
    print(f"Colonnes              : {metadata['column_count']}")
    print(f"Groupes de lignes     : {metadata['row_group_count']}")
    print(f"Taille                : {metadata['size_bytes']} octets")


def print_summary(
    source_path: Path,
    output_directory: Path,
    manifest_path: Path,
    source_row_count: int,
    source_size_bytes: int,
    sla_minutes: float,
    compression: str,
    gold_version: str,
    fact_metadata: dict[str, Any],
    courier_metadata: dict[str, Any],
    city_metadata: dict[str, Any],
    fact_kpis: dict[str, int],
) -> None:
    """Affiche le résumé de la production Gold."""

    total_gold_size = (
        int(fact_metadata["size_bytes"])
        + int(courier_metadata["size_bytes"])
        + int(city_metadata["size_bytes"])
    )

    size_ratio = (
        round(
            total_gold_size
            / source_size_bytes,
            4,
        )
        if source_size_bytes
        else None
    )

    print()
    print("=" * 78)
    print("SMARTLOGIX 360 - PRODUCTION GOLD LADE-D")
    print("=" * 78)
    print(f"Source Silver         : {source_path}")
    print(f"Dossier Gold          : {output_directory}")
    print(f"Version Gold          : {gold_version}")
    print(f"SLA                    : {sla_minutes} minutes")
    print(f"Compression            : {compression}")
    print(f"Lignes Silver          : {source_row_count}")
    print(f"Taille Silver          : {source_size_bytes} octets")
    print(f"Taille totale Gold     : {total_gold_size} octets")
    print(f"Ratio Gold/Silver      : {size_ratio}")

    print_table_summary(
        table_name="delivery_fact",
        metadata=fact_metadata,
    )

    print_table_summary(
        table_name="courier_daily_performance",
        metadata=courier_metadata,
    )

    print_table_summary(
        table_name="city_daily_performance",
        metadata=city_metadata,
    )

    print()
    print("INDICATEURS GLOBAUX")
    print("-" * 78)
    print(
        "Livraisons totales    : "
        f"{fact_kpis['delivery_count']}"
    )
    print(
        "Durées valides        : "
        f"{fact_kpis['valid_duration_count']}"
    )
    print(
        "Dans le SLA           : "
        f"{fact_kpis['within_sla_count']}"
    )
    print(
        "Livraisons en retard  : "
        f"{fact_kpis['late_delivery_count']}"
    )
    print(
        "GPS complet           : "
        f"{fact_kpis['complete_gps_count']}"
    )
    print(
        "Alertes qualité       : "
        f"{fact_kpis['quality_warning_count']}"
    )
    print(f"Manifeste Gold        : {manifest_path}")
    print("=" * 78)


def main() -> int:
    """Produit les trois tables Gold depuis Silver."""

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
            / "silver"
            / DEFAULT_RELATIVE_SILVER_INPUT
        )
    ).resolve()

    output_directory = (
        arguments.output_directory
        if arguments.output_directory is not None
        else build_default_output_directory(
            project_data_root
        )
    ).resolve()

    manifest_path = (
        arguments.build_manifest
        if arguments.build_manifest is not None
        else build_default_manifest_path(
            project_data_root
        )
    ).resolve()

    try:
        silver_dataframe = load_silver_parquet(
            source_path
        )

        builder = LaDeGoldBuilder(
            sla_minutes=arguments.sla_minutes,
            compression=arguments.compression,
        )

        result = builder.build(
            dataframe=silver_dataframe,
            source_path=source_path,
            output_directory=output_directory,
        )

        delivery_fact_path = Path(
            result.delivery_fact.output_path
        )

        courier_daily_path = Path(
            result.courier_daily_performance.output_path
        )

        city_daily_path = Path(
            result.city_daily_performance.output_path
        )

        fact_metadata = read_parquet_metadata(
            delivery_fact_path
        )

        courier_metadata = read_parquet_metadata(
            courier_daily_path
        )

        city_metadata = read_parquet_metadata(
            city_daily_path
        )

        source_row_count = int(
            len(silver_dataframe)
        )

        if (
            int(fact_metadata["row_count"])
            != source_row_count
        ):
            raise LaDeGoldBuildError(
                "La table delivery_fact ne contient pas "
                "le même nombre de lignes que Silver."
            )

        courier_order_total = (
            calculate_aggregate_order_total(
                courier_daily_path
            )
        )

        city_order_total = (
            calculate_aggregate_order_total(
                city_daily_path
            )
        )

        courier_eligible_row_count = (
            calculate_courier_eligible_row_count(
                delivery_fact_path
            )
        )

        if (
            courier_order_total
            != courier_eligible_row_count
        ):
            raise LaDeGoldBuildError(
                "La somme orders_total de la table coursier "
                "ne correspond pas au nombre de livraisons "
                "avec courier_id > 0."
            )

        if city_order_total != source_row_count:
            raise LaDeGoldBuildError(
                "La somme orders_total de la table ville "
                "ne correspond pas au nombre de livraisons."
            )

        fact_kpis = calculate_fact_kpis(
            delivery_fact_path
        )

        if (
            fact_kpis["delivery_count"]
            != source_row_count
        ):
            raise LaDeGoldBuildError(
                "Le total delivery_count ne correspond pas "
                "au nombre de lignes Silver."
            )

        source_size_bytes = int(
            source_path.stat().st_size
        )

        manifest_content = result.to_dict()

        manifest_content.update(
            {
                "source_row_count": (
                    source_row_count
                ),
                "source_size_bytes": (
                    source_size_bytes
                ),
                "delivery_fact_metadata": (
                    fact_metadata
                ),
                "courier_daily_metadata": (
                    courier_metadata
                ),
                "city_daily_metadata": (
                    city_metadata
                ),
                "courier_orders_total": (
                    courier_order_total
                ),
                "city_orders_total": (
                    city_order_total
                ),
                "global_kpis": fact_kpis,
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
        LaDeGoldBuildError,
    ) as error:
        print()
        print(f"Erreur : {error}")
        return 1

    print_summary(
        source_path=source_path,
        output_directory=output_directory,
        manifest_path=written_manifest_path,
        source_row_count=source_row_count,
        source_size_bytes=source_size_bytes,
        sla_minutes=result.sla_minutes,
        compression=result.compression,
        gold_version=result.gold_version,
        fact_metadata=fact_metadata,
        courier_metadata=courier_metadata,
        city_metadata=city_metadata,
        fact_kpis=fact_kpis,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())