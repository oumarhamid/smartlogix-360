from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError

from smartlogix.common import configure_logging
from smartlogix.config import get_settings
from smartlogix.storage import (
    PostgresGoldLoader,
    PostgresGoldLoadError,
)

GOLD_TABLE_FILES = {
    "delivery_fact": "delivery_fact.parquet",
    "courier_daily_performance": (
        "courier_daily_performance.parquet"
    ),
    "city_daily_performance": (
        "city_daily_performance.parquet"
    ),
}


def parse_arguments() -> argparse.Namespace:
    """Analyse les arguments de la ligne de commande."""

    parser = argparse.ArgumentParser(
        description=(
            "Charge les trois tables Gold LaDe-D "
            "dans PostgreSQL."
        )
    )

    parser.add_argument(
        "--gold-directory",
        type=Path,
        default=None,
        help=(
            "Dossier contenant les Parquet Gold. "
            "Par défaut : data/gold/lade/delivery."
        ),
    )

    parser.add_argument(
        "--schema",
        default="analytics",
        help=(
            "Schéma PostgreSQL cible. "
            "Par défaut : analytics."
        ),
    )

    parser.add_argument(
        "--chunksize",
        type=int,
        default=1000,
        help=(
            "Nombre de lignes insérées par lot. "
            "Par défaut : 1000."
        ),
    )

    parser.add_argument(
        "--load-manifest",
        type=Path,
        default=None,
        help=(
            "Chemin facultatif du manifeste JSON "
            "du chargement PostgreSQL."
        ),
    )

    arguments = parser.parse_args()

    if arguments.chunksize <= 0:
        parser.error(
            "--chunksize doit être strictement positif."
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


def build_default_gold_directory(
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
    """Construit le chemin du manifeste PostgreSQL."""

    return (
        project_data_root
        / "gold"
        / "lade"
        / "_metadata"
        / "postgres"
        / "delivery_gold.postgres.json"
    )


def load_gold_parquet(
    file_path: Path,
    table_name: str,
) -> pd.DataFrame:
    """Charge une table Gold depuis son fichier Parquet."""

    resolved_path = file_path.resolve()

    if not resolved_path.is_file():
        raise FileNotFoundError(
            f"Le fichier Gold de {table_name} "
            f"est introuvable : {resolved_path}"
        )

    try:
        dataframe = pd.read_parquet(
            resolved_path,
            engine="pyarrow",
        )
    except Exception as error:
        raise PostgresGoldLoadError(
            f"Impossible de lire le fichier Gold "
            f"{resolved_path}."
        ) from error

    if dataframe.empty:
        raise PostgresGoldLoadError(
            f"Le fichier Gold {table_name} est vide."
        )

    return dataframe


def load_gold_tables(
    gold_directory: Path,
) -> tuple[
    dict[str, pd.DataFrame],
    dict[str, dict[str, Any]],
]:
    """Charge les trois fichiers Parquet Gold."""

    dataframes: dict[str, pd.DataFrame] = {}
    sources: dict[str, dict[str, Any]] = {}

    for table_name, file_name in (
        GOLD_TABLE_FILES.items()
    ):
        file_path = (
            gold_directory
            / file_name
        ).resolve()

        dataframe = load_gold_parquet(
            file_path=file_path,
            table_name=table_name,
        )

        dataframes[table_name] = dataframe

        sources[table_name] = {
            "path": str(file_path),
            "row_count": int(
                len(dataframe)
            ),
            "column_count": int(
                len(dataframe.columns)
            ),
            "size_bytes": int(
                file_path.stat().st_size
            ),
        }

    return dataframes, sources


def qualified_table_name(
    schema_name: str,
    table_name: str,
) -> str:
    """Construit un nom de table PostgreSQL qualifié."""

    return (
        f'"{schema_name}".'
        f'"{table_name}"'
    )


def verify_loaded_tables(
    loader: PostgresGoldLoader,
    expected_row_counts: dict[str, int],
) -> dict[str, Any]:
    """Vérifie les tables, volumes, index et KPI chargés."""

    table_verifications: dict[
        str,
        dict[str, Any],
    ] = {}

    schema_name = loader.schema_name

    try:
        with loader.engine.connect() as connection:
            database_inspector = inspect(
                connection
            )

            for table_name in GOLD_TABLE_FILES:
                if not database_inspector.has_table(
                    table_name,
                    schema=schema_name,
                ):
                    raise PostgresGoldLoadError(
                        "La table PostgreSQL est absente : "
                        f"{schema_name}.{table_name}"
                    )

                qualified_table = (
                    qualified_table_name(
                        schema_name=schema_name,
                        table_name=table_name,
                    )
                )

                row_count = int(
                    connection.execute(
                        text(
                            f"SELECT COUNT(*) "
                            f"FROM {qualified_table}"
                        )
                    ).scalar_one()
                )

                expected_row_count = (
                    expected_row_counts[
                        table_name
                    ]
                )

                if row_count != expected_row_count:
                    raise PostgresGoldLoadError(
                        f"Le nombre de lignes de "
                        f"{schema_name}.{table_name} "
                        f"est incorrect : {row_count} "
                        f"au lieu de "
                        f"{expected_row_count}."
                    )

                columns = (
                    database_inspector.get_columns(
                        table_name,
                        schema=schema_name,
                    )
                )

                indexes = (
                    database_inspector.get_indexes(
                        table_name,
                        schema=schema_name,
                    )
                )

                table_verifications[
                    table_name
                ] = {
                    "row_count": row_count,
                    "column_count": len(columns),
                    "columns": [
                        column["name"]
                        for column in columns
                    ],
                    "index_count": len(indexes),
                    "indexes": [
                        {
                            "name": index["name"],
                            "columns": (
                                index[
                                    "column_names"
                                ]
                            ),
                            "unique": bool(
                                index["unique"]
                            ),
                        }
                        for index in indexes
                    ],
                }

            fact_table = qualified_table_name(
                schema_name=schema_name,
                table_name="delivery_fact",
            )

            global_kpi_row = (
                connection.execute(
                    text(
                        "SELECT "
                        "COALESCE(SUM(delivery_count), 0) "
                        "AS deliveries_total, "
                        "COALESCE("
                        "SUM(delivery_count) FILTER "
                        "(WHERE courier_id > 0), 0"
                        ") AS courier_eligible_deliveries_total, "
                        "COALESCE("
                        "SUM(delivery_count) FILTER "
                        "(WHERE courier_id = 0), 0"
                        ") AS zero_courier_deliveries_total, "
                        "COUNT(*) FILTER "
                        "(WHERE is_valid_duration) "
                        "AS valid_duration_count, "
                        "COUNT(*) FILTER "
                        "(WHERE is_within_sla) "
                        "AS within_sla_count, "
                        "COUNT(*) FILTER "
                        "(WHERE is_late_delivery) "
                        "AS late_delivery_count, "
                        "COUNT(*) FILTER "
                        "(WHERE has_complete_gps) "
                        "AS complete_gps_count, "
                        "COUNT(*) FILTER "
                        "(WHERE is_quality_warning) "
                        "AS quality_warning_count "
                        f"FROM {fact_table}"
                    )
                )
                .mappings()
                .one()
            )

            courier_table = qualified_table_name(
                schema_name=schema_name,
                table_name=(
                    "courier_daily_performance"
                ),
            )

            city_table = qualified_table_name(
                schema_name=schema_name,
                table_name=(
                    "city_daily_performance"
                ),
            )

            courier_orders_total = int(
                connection.execute(
                    text(
                        "SELECT "
                        "COALESCE(SUM(orders_total), 0) "
                        f"FROM {courier_table}"
                    )
                ).scalar_one()
            )

            city_orders_total = int(
                connection.execute(
                    text(
                        "SELECT "
                        "COALESCE(SUM(orders_total), 0) "
                        f"FROM {city_table}"
                    )
                ).scalar_one()
            )

    except SQLAlchemyError as error:
        raise PostgresGoldLoadError(
            "La vérification PostgreSQL a échoué."
        ) from error

    delivery_count = int(
        global_kpi_row[
            "deliveries_total"
        ]
    )

    courier_eligible_delivery_count = int(
        global_kpi_row[
            "courier_eligible_deliveries_total"
        ]
    )

    zero_courier_delivery_count = int(
        global_kpi_row[
            "zero_courier_deliveries_total"
        ]
    )

    if (
        courier_eligible_delivery_count
        + zero_courier_delivery_count
        != delivery_count
    ):
        raise PostgresGoldLoadError(
            "La décomposition des livraisons "
            "selon courier_id ne correspond pas "
            "à delivery_fact."
        )

    if (
        courier_orders_total
        != courier_eligible_delivery_count
    ):
        raise PostgresGoldLoadError(
            "Le total des commandes par coursier "
            "ne correspond pas aux livraisons avec "
            "courier_id > 0 dans delivery_fact."
        )

    if city_orders_total != delivery_count:
        raise PostgresGoldLoadError(
            "Le total des commandes par ville "
            "ne correspond pas à delivery_fact."
        )

    global_kpis = {
        key: int(value or 0)
        for key, value in global_kpi_row.items()
    }

    return {
        "tables": table_verifications,
        "global_kpis": global_kpis,
        "courier_orders_total": (
            courier_orders_total
        ),
        "city_orders_total": (
            city_orders_total
        ),
    }


def write_load_manifest(
    content: dict[str, Any],
    output_path: Path,
) -> Path:
    """Écrit atomiquement le manifeste PostgreSQL."""

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
    schema_name: str,
    table_name: str,
    source: dict[str, Any],
    verification: dict[str, Any],
    replaced_existing_data: bool,
) -> None:
    """Affiche le résumé du chargement d'une table."""

    print()
    print(
        f"Table                 : "
        f"{schema_name}.{table_name}"
    )
    print(f"Source                : {source['path']}")
    print(f"Lignes source         : {source['row_count']}")
    print(
        "Lignes PostgreSQL     : "
        f"{verification['row_count']}"
    )
    print(
        "Colonnes PostgreSQL   : "
        f"{verification['column_count']}"
    )
    print(
        "Index PostgreSQL      : "
        f"{verification['index_count']}"
    )
    print(
        "Données remplacées    : "
        f"{replaced_existing_data}"
    )


def print_summary(
    database_host: str,
    database_port: int,
    database_name: str,
    database_user: str,
    schema_name: str,
    chunksize: int,
    sources: dict[str, dict[str, Any]],
    verification: dict[str, Any],
    load_result: dict[str, Any],
    manifest_path: Path,
) -> None:
    """Affiche le résumé global du chargement."""

    print()
    print("=" * 78)
    print(
        "SMARTLOGIX 360 - "
        "CHARGEMENT GOLD VERS POSTGRESQL"
    )
    print("=" * 78)
    print(f"Hôte PostgreSQL       : {database_host}")
    print(f"Port PostgreSQL       : {database_port}")
    print(f"Base                  : {database_name}")
    print(f"Utilisateur           : {database_user}")
    print(f"Schéma                : {schema_name}")
    print(f"Taille des lots       : {chunksize}")

    for table_name in GOLD_TABLE_FILES:
        print_table_summary(
            schema_name=schema_name,
            table_name=table_name,
            source=sources[table_name],
            verification=(
                verification["tables"][
                    table_name
                ]
            ),
            replaced_existing_data=bool(
                load_result[table_name][
                    "replaced_existing_data"
                ]
            ),
        )

    global_kpis = verification[
        "global_kpis"
    ]

    print()
    print("INDICATEURS VÉRIFIÉS DANS POSTGRESQL")
    print("-" * 78)
    print(
        "Livraisons totales    : "
        f"{global_kpis['deliveries_total']}"
    )
    print(
        "Avec courier_id > 0   : "
        f"{global_kpis['courier_eligible_deliveries_total']}"
    )
    print(
        "Avec courier_id = 0   : "
        f"{global_kpis['zero_courier_deliveries_total']}"
    )
    print(
        "Durées valides        : "
        f"{global_kpis['valid_duration_count']}"
    )
    print(
        "Dans le SLA           : "
        f"{global_kpis['within_sla_count']}"
    )
    print(
        "Livraisons en retard  : "
        f"{global_kpis['late_delivery_count']}"
    )
    print(
        "GPS complet           : "
        f"{global_kpis['complete_gps_count']}"
    )
    print(
        "Alertes qualité       : "
        f"{global_kpis['quality_warning_count']}"
    )
    print(
        "Agrégées par coursier : "
        f"{verification['courier_orders_total']}"
    )
    print(
        "Agrégées par ville    : "
        f"{verification['city_orders_total']}"
    )
    print(f"Manifeste             : {manifest_path}")
    print("=" * 78)


def main() -> int:
    """Charge les tables Gold dans PostgreSQL."""

    configure_logging()

    arguments = parse_arguments()
    settings = get_settings()

    project_data_root = find_project_data_root(
        settings.resolved_lade_data_dir
    )

    gold_directory = (
        arguments.gold_directory
        if arguments.gold_directory is not None
        else build_default_gold_directory(
            project_data_root
        )
    ).resolve()

    manifest_path = (
        arguments.load_manifest
        if arguments.load_manifest is not None
        else build_default_manifest_path(
            project_data_root
        )
    ).resolve()

    try:
        dataframes, sources = load_gold_tables(
            gold_directory
        )

        expected_row_counts = {
            table_name: int(
                len(dataframe)
            )
            for table_name, dataframe in (
                dataframes.items()
            )
        }

        with PostgresGoldLoader(
            database_url=settings.postgres_url,
            schema_name=arguments.schema,
            chunksize=arguments.chunksize,
        ) as loader:
            result = loader.load_gold_tables(
                delivery_fact=(
                    dataframes["delivery_fact"]
                ),
                courier_daily_performance=(
                    dataframes[
                        "courier_daily_performance"
                    ]
                ),
                city_daily_performance=(
                    dataframes[
                        "city_daily_performance"
                    ]
                ),
            )

            verification = verify_loaded_tables(
                loader=loader,
                expected_row_counts=(
                    expected_row_counts
                ),
            )

        result_data = result.to_dict()

        manifest_content = {
            **result_data,
            "database_host": (
                settings.postgres_host
            ),
            "database_port": (
                settings.postgres_port
            ),
            "chunksize": arguments.chunksize,
            "gold_directory": str(
                gold_directory
            ),
            "sources": sources,
            "verification": verification,
        }

        written_manifest_path = (
            write_load_manifest(
                content=manifest_content,
                output_path=manifest_path,
            )
        )

    except (
        FileNotFoundError,
        ValueError,
        OSError,
        SQLAlchemyError,
        PostgresGoldLoadError,
    ) as error:
        print()
        print(f"Erreur : {error}")
        return 1

    print_summary(
        database_host=settings.postgres_host,
        database_port=settings.postgres_port,
        database_name=result.database_name,
        database_user=result.database_user,
        schema_name=result.schema_name,
        chunksize=arguments.chunksize,
        sources=sources,
        verification=verification,
        load_result=result_data,
        manifest_path=written_manifest_path,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())