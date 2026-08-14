from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

GOLD_ROOT = Path("data/gold/lade/delivery")

CITIES = (
    "jl",
    "yt",
    "cq",
    "sh",
    "hz",
)

TABLES = (
    "delivery_fact.parquet",
    "courier_daily_performance.parquet",
    "city_daily_performance.parquet",
)


def consolidate_table(table_name: str) -> int:
    """Fusionne une table Gold des cinq villes."""

    input_paths = [
        GOLD_ROOT / "by_city" / city / table_name
        for city in CITIES
    ]

    for path in input_paths:
        if not path.is_file():
            raise FileNotFoundError(
                f"Fichier introuvable : {path}"
            )

    output_path = GOLD_ROOT / table_name
    temporary_path = GOLD_ROOT / (
        f".{table_name}.tmp"
    )

    first_file = pq.ParquetFile(
        input_paths[0]
    )

    target_schema = first_file.schema_arrow

    writer = pq.ParquetWriter(
        temporary_path,
        schema=target_schema,
        compression="zstd",
    )

    total_rows = 0

    try:
        for path in input_paths:
            parquet_file = pq.ParquetFile(
                path
            )

            if not parquet_file.schema_arrow.equals(
                target_schema,
                check_metadata=False,
            ):
                raise RuntimeError(
                    "Schéma incompatible : "
                    f"{path}"
                )

            for batch in parquet_file.iter_batches(
                batch_size=100_000
            ):
                table = pa.Table.from_batches(
                    [batch],
                    schema=target_schema,
                )

                writer.write_table(table)

                total_rows += table.num_rows

    finally:
        writer.close()

    written_rows = int(
        pq.ParquetFile(
            temporary_path
        ).metadata.num_rows
    )

    if written_rows != total_rows:
        temporary_path.unlink(
            missing_ok=True
        )

        raise RuntimeError(
            f"{table_name} : "
            "nombre de lignes incorrect."
        )

    temporary_path.replace(
        output_path
    )

    print(
        f"{table_name}: "
        f"{total_rows:,} lignes"
    )

    return total_rows


def main() -> None:
    print()
    print("=" * 70)
    print(
        "SMARTLOGIX 360 - "
        "CONSOLIDATION GOLD MULTI-VILLES"
    )
    print("=" * 70)

    results = {}

    for table_name in TABLES:
        results[table_name] = (
            consolidate_table(
                table_name
            )
        )

    print()
    print("=" * 70)
    print("CONSOLIDATION TERMINÉE")
    print("=" * 70)

    for table_name, row_count in (
        results.items()
    ):
        print(
            f"{table_name:<40}"
            f"{row_count:>12,}"
        )


if __name__ == "__main__":
    main()