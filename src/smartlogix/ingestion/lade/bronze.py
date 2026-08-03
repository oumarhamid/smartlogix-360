from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from string import hexdigits
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from smartlogix.common import get_logger
from smartlogix.ingestion.lade.quality import (
    LaDeQualityReport,
)

logger = get_logger(__name__)


class LaDeBronzeBuildError(RuntimeError):
    """Erreur rencontrée pendant la création de la couche Bronze."""


BRONZE_WARNING_COLUMNS = {
    "missing_accept_gps": "_warning_missing_accept_gps",
    "near_zero_accept_gps": "_warning_near_zero_accept_gps",
    "near_zero_delivery_gps": "_warning_near_zero_delivery_gps",
    "zero_or_negative_duration": (
        "_warning_zero_or_negative_duration"
    ),
    "long_delivery_duration": (
        "_warning_long_delivery_duration"
    ),
    "partition_accept_date_mismatch": (
        "_warning_partition_mismatch"
    ),
}


@dataclass(frozen=True, slots=True)
class LaDeBronzeResult:
    """Résultat de la production d'un fichier Bronze."""

    source_path: str
    output_path: str
    source_sha256: str
    dataset_revision: str
    created_at: datetime
    row_count: int
    column_count: int
    warning_row_count: int
    valid_row_count: int
    compression: str
    parquet_size_bytes: int

    def to_dict(self) -> dict[str, Any]:
        """Convertit le résultat en dictionnaire sérialisable."""

        data = asdict(self)
        data["created_at"] = self.created_at.isoformat()

        return data


class LaDeBronzeBuilder:
    """Prépare et écrit les livraisons LaDe en couche Bronze."""

    def __init__(
        self,
        compression: str = "zstd",
    ) -> None:
        self.compression = compression

    @staticmethod
    def _validate_source_sha256(
        source_sha256: str,
    ) -> None:
        """Vérifie le format du SHA-256 source."""

        is_valid = (
            len(source_sha256) == 64
            and all(
                character in hexdigits
                for character in source_sha256
            )
        )

        if not is_valid:
            raise LaDeBronzeBuildError(
                "Le SHA-256 source doit contenir "
                "exactement 64 caractères hexadécimaux."
            )

    @staticmethod
    def _normalize_ingested_at(
        ingested_at: datetime | None,
    ) -> pd.Timestamp:
        """Normalise la date d'ingestion en UTC."""

        timestamp = pd.Timestamp(
            ingested_at or datetime.now(UTC)
        )

        if timestamp.tzinfo is None:
            return timestamp.tz_localize("UTC")

        return timestamp.tz_convert("UTC")

    @staticmethod
    def _parse_month_day_time(
        series: pd.Series,
    ) -> pd.Series:
        """Convertit une date LaDe avec l'année technique 2000."""

        normalized = (
            "2000-"
            + series.astype("string")
        )

        return pd.to_datetime(
            normalized,
            format="%Y-%m-%d %H:%M:%S",
            errors="coerce",
            utc=True,
        )

    @classmethod
    def _calculate_delivery_duration(
        cls,
        dataframe: pd.DataFrame,
    ) -> pd.Series:
        """Calcule la durée de livraison en minutes."""

        accept_timestamp = cls._parse_month_day_time(
            dataframe["accept_time"]
        )

        delivery_timestamp = cls._parse_month_day_time(
            dataframe["delivery_time"]
        )

        durations = (
            delivery_timestamp - accept_timestamp
        ).dt.total_seconds() / 60

        overnight_mask = durations < 0

        return durations.where(
            ~overnight_mask,
            durations + (24 * 60),
        )

    @staticmethod
    def _add_warning_flags(
        dataframe: pd.DataFrame,
        quality_report: LaDeQualityReport,
    ) -> pd.DataFrame:
        """Ajoute un indicateur booléen pour chaque règle qualité."""

        transformed = dataframe.copy()

        for flag_column in BRONZE_WARNING_COLUMNS.values():
            transformed[flag_column] = False

        for warning in quality_report.warnings:
            flag_column = BRONZE_WARNING_COLUMNS.get(
                warning.rule
            )

            if flag_column is None:
                continue

            warning_mask = transformed.index.isin(
                warning.row_indices
            )

            transformed.loc[
                warning_mask,
                flag_column,
            ] = True

        warning_columns = list(
            BRONZE_WARNING_COLUMNS.values()
        )

        transformed["_quality_warning_count"] = (
            transformed[warning_columns]
            .sum(axis=1)
            .astype("int16")
        )

        transformed["_quality_status"] = pd.Series(
            "valid",
            index=transformed.index,
            dtype="string",
        )

        transformed.loc[
            transformed["_quality_warning_count"].gt(0),
            "_quality_status",
        ] = "warning"

        return transformed

    def transform(
        self,
        dataframe: pd.DataFrame,
        quality_report: LaDeQualityReport,
        source_path: Path,
        source_sha256: str,
        dataset_revision: str,
        ingested_at: datetime | None = None,
    ) -> pd.DataFrame:
        """Transforme les données validées vers le format Bronze."""

        if not quality_report.blocking_passed:
            raise LaDeBronzeBuildError(
                "La couche Bronze ne peut pas être produite "
                "car le rapport contient des erreurs bloquantes."
            )

        if quality_report.row_count != len(dataframe):
            raise LaDeBronzeBuildError(
                "Le nombre de lignes du rapport qualité ne "
                "correspond pas au DataFrame source."
            )

        self._validate_source_sha256(source_sha256)

        if not dataset_revision.strip():
            raise LaDeBronzeBuildError(
                "La révision du dataset est obligatoire."
            )

        transformed = self._add_warning_flags(
            dataframe=dataframe,
            quality_report=quality_report,
        )

        transformed["accept_timestamp"] = (
            self._parse_month_day_time(
                transformed["accept_time"]
            )
        )

        transformed["accept_gps_timestamp"] = (
            self._parse_month_day_time(
                transformed["accept_gps_time"]
            )
        )

        transformed["delivery_timestamp"] = (
            self._parse_month_day_time(
                transformed["delivery_time"]
            )
        )

        transformed["delivery_gps_timestamp"] = (
            self._parse_month_day_time(
                transformed["delivery_gps_time"]
            )
        )

        transformed["delivery_duration_minutes"] = (
            self._calculate_delivery_duration(
                transformed
            )
        )

        resolved_source_path = source_path.resolve()

        ingestion_timestamp = (
            self._normalize_ingested_at(ingested_at)
        )

        transformed["_source_row_number"] = pd.Series(
            range(2, len(transformed) + 2),
            index=transformed.index,
            dtype="int64",
        )

        transformed["_source_file"] = (
            resolved_source_path.name
        )

        transformed["_source_path"] = str(
            resolved_source_path
        )

        transformed["_source_sha256"] = (
            source_sha256.lower()
        )

        transformed["_dataset_revision"] = (
            dataset_revision
        )

        transformed["_ingested_at"] = (
            ingestion_timestamp
        )

        transformed["_quality_contract_passed"] = True

        logger.info(
            "lade_bronze_transformation_completed",
            source_path=str(resolved_source_path),
            row_count=int(len(transformed)),
            column_count=int(len(transformed.columns)),
            warning_row_count=int(
                transformed["_quality_status"]
                .eq("warning")
                .sum()
            ),
        )

        return transformed

    def write_parquet(
        self,
        dataframe: pd.DataFrame,
        output_path: Path,
    ) -> Path:
        """Écrit atomiquement le DataFrame en Parquet."""

        resolved_output_path = output_path.resolve()

        resolved_output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        temporary_path = resolved_output_path.with_name(
            f"{resolved_output_path.stem}"
            f".tmp{resolved_output_path.suffix}"
        )

        logger.info(
            "lade_bronze_parquet_write_started",
            output_path=str(resolved_output_path),
            row_count=int(len(dataframe)),
            compression=self.compression,
        )

        try:
            dataframe.to_parquet(
                temporary_path,
                engine="pyarrow",
                compression=self.compression,
                index=False,
            )

            with temporary_path.open("rb") as parquet_stream:
                parquet_file = pq.ParquetFile(
                    parquet_stream
                )

                parquet_row_count = int(
                    parquet_file.metadata.num_rows
                )

                parquet_columns = set(
                    parquet_file.schema_arrow.names
                )

            if parquet_row_count != len(dataframe):
                raise LaDeBronzeBuildError(
                    "Le nombre de lignes du fichier Parquet "
                    "ne correspond pas au DataFrame Bronze."
                )

            missing_columns = set(
                dataframe.columns
            ).difference(parquet_columns)

            if missing_columns:
                missing_text = ", ".join(
                    sorted(missing_columns)
                )

                raise LaDeBronzeBuildError(
                    "Des colonnes sont absentes du fichier "
                    f"Parquet : {missing_text}"
                )

            temporary_path.replace(
                resolved_output_path
            )

        except Exception as error:
            try:
                temporary_path.unlink(
                    missing_ok=True
                )
            except PermissionError:
                logger.warning(
                    "lade_bronze_temporary_cleanup_failed",
                    temporary_path=str(temporary_path),
                )

            if isinstance(
                error,
                LaDeBronzeBuildError,
            ):
                raise

            logger.exception(
                "lade_bronze_parquet_write_failed",
                output_path=str(resolved_output_path),
                error_type=type(error).__name__,
            )

            raise LaDeBronzeBuildError(
                "Impossible d'écrire le fichier Bronze : "
                f"{resolved_output_path}"
            ) from error

        logger.info(
            "lade_bronze_parquet_write_completed",
            output_path=str(resolved_output_path),
            row_count=int(len(dataframe)),
            parquet_size_bytes=(
                resolved_output_path.stat().st_size
            ),
        )

        return resolved_output_path

    def build(
        self,
        dataframe: pd.DataFrame,
        quality_report: LaDeQualityReport,
        source_path: Path,
        output_path: Path,
        source_sha256: str,
        dataset_revision: str,
        ingested_at: datetime | None = None,
    ) -> LaDeBronzeResult:
        """Transforme puis écrit le fichier Bronze."""

        created_at = (
            ingested_at or datetime.now(UTC)
        )

        transformed = self.transform(
            dataframe=dataframe,
            quality_report=quality_report,
            source_path=source_path,
            source_sha256=source_sha256,
            dataset_revision=dataset_revision,
            ingested_at=created_at,
        )

        written_path = self.write_parquet(
            dataframe=transformed,
            output_path=output_path,
        )

        warning_row_count = int(
            transformed["_quality_status"]
            .eq("warning")
            .sum()
        )

        row_count = int(len(transformed))

        return LaDeBronzeResult(
            source_path=str(
                source_path.resolve()
            ),
            output_path=str(written_path),
            source_sha256=source_sha256.lower(),
            dataset_revision=dataset_revision,
            created_at=created_at,
            row_count=row_count,
            column_count=int(
                len(transformed.columns)
            ),
            warning_row_count=warning_row_count,
            valid_row_count=(
                row_count - warning_row_count
            ),
            compression=self.compression,
            parquet_size_bytes=(
                written_path.stat().st_size
            ),
        )