from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from smartlogix.common import get_logger

logger = get_logger(__name__)

SILVER_VERSION = "1.0"

SILVER_REQUIRED_COLUMNS = (
    "order_id",
    "region_id",
    "city",
    "courier_id",
    "lng",
    "lat",
    "aoi_id",
    "aoi_type",
    "accept_time",
    "accept_gps_time",
    "accept_gps_lng",
    "accept_gps_lat",
    "delivery_time",
    "delivery_gps_time",
    "delivery_gps_lng",
    "delivery_gps_lat",
    "ds",
    "accept_timestamp",
    "delivery_timestamp",
    "delivery_duration_minutes",
    "_source_row_number",
    "_source_file",
    "_source_sha256",
    "_dataset_revision",
    "_ingested_at",
    "_quality_status",
    "_quality_contract_passed",
)


class LaDeSilverBuildError(RuntimeError):
    """Erreur rencontrée pendant la création de la couche Silver."""


@dataclass(frozen=True, slots=True)
class LaDeSilverResult:
    """Résumé de la production d'un fichier Silver."""

    source_path: str
    output_path: str
    created_at: datetime
    silver_version: str
    row_count: int
    column_count: int
    quality_warning_row_count: int
    gps_issue_row_count: int
    duration_issue_row_count: int
    compression: str
    parquet_size_bytes: int

    def to_dict(self) -> dict[str, Any]:
        """Convertit le résultat en dictionnaire sérialisable."""

        data = asdict(self)
        data["created_at"] = self.created_at.isoformat()

        return data


class LaDeSilverBuilder:
    """Nettoie et enrichit les données Bronze LaDe-D."""

    def __init__(
        self,
        compression: str = "zstd",
        near_zero_coordinate: float = 1.0,
        long_duration_minutes: float = 24 * 60,
    ) -> None:
        if near_zero_coordinate <= 0:
            raise ValueError(
                "near_zero_coordinate doit être strictement positif."
            )

        if long_duration_minutes <= 0:
            raise ValueError(
                "long_duration_minutes doit être strictement positif."
            )

        self.compression = compression
        self.near_zero_coordinate = near_zero_coordinate
        self.long_duration_minutes = long_duration_minutes

    @staticmethod
    def _normalize_utc_timestamp(
        value: datetime | None,
    ) -> pd.Timestamp:
        """Normalise une date technique en UTC."""

        timestamp = pd.Timestamp(
            value or datetime.now(UTC)
        )

        if timestamp.tzinfo is None:
            return timestamp.tz_localize("UTC")

        return timestamp.tz_convert("UTC")

    @staticmethod
    def _validate_input(
        dataframe: pd.DataFrame,
    ) -> None:
        """Vérifie que le DataFrame Bronze est exploitable."""

        missing_columns = set(
            SILVER_REQUIRED_COLUMNS
        ).difference(dataframe.columns)

        if missing_columns:
            missing_text = ", ".join(
                sorted(missing_columns)
            )

            raise LaDeSilverBuildError(
                "Des colonnes Bronze obligatoires sont absentes : "
                f"{missing_text}"
            )

        if dataframe["order_id"].isna().any():
            raise LaDeSilverBuildError(
                "La colonne order_id contient des valeurs nulles."
            )

        if dataframe["order_id"].duplicated().any():
            raise LaDeSilverBuildError(
                "La colonne order_id contient des doublons."
            )

        contract_status = (
            dataframe["_quality_contract_passed"]
            .fillna(False)
            .astype(bool)
        )

        if not bool(contract_status.all()):
            raise LaDeSilverBuildError(
                "Le DataFrame Bronze contient des lignes qui "
                "n'ont pas validé le contrat qualité."
            )

    def _clean_coordinate_pair(
        self,
        longitude: pd.Series,
        latitude: pd.Series,
    ) -> tuple[pd.Series, pd.Series, pd.Series]:
        """Nettoie une paire longitude/latitude."""

        normalized_longitude = pd.to_numeric(
            longitude,
            errors="coerce",
        )

        normalized_latitude = pd.to_numeric(
            latitude,
            errors="coerce",
        )

        valid_mask = (
            normalized_longitude.notna()
            & normalized_latitude.notna()
            & normalized_longitude.abs().ge(
                self.near_zero_coordinate
            )
            & normalized_latitude.abs().ge(
                self.near_zero_coordinate
            )
            & normalized_longitude.between(
                -180,
                180,
            )
            & normalized_latitude.between(
                -90,
                90,
            )
        )

        clean_longitude = (
            normalized_longitude
            .where(valid_mask)
            .astype("Float64")
        )

        clean_latitude = (
            normalized_latitude
            .where(valid_mask)
            .astype("Float64")
        )

        return (
            clean_longitude,
            clean_latitude,
            valid_mask.astype(bool),
        )

    @staticmethod
    def _build_gps_status(
        accept_gps_valid: pd.Series,
        delivery_gps_valid: pd.Series,
    ) -> pd.Series:
        """Construit le statut de disponibilité GPS."""

        status = pd.Series(
            "complete",
            index=accept_gps_valid.index,
            dtype="string",
        )

        status.loc[
            ~accept_gps_valid
            & delivery_gps_valid
        ] = "missing_or_invalid_accept"

        status.loc[
            accept_gps_valid
            & ~delivery_gps_valid
        ] = "missing_or_invalid_delivery"

        status.loc[
            ~accept_gps_valid
            & ~delivery_gps_valid
        ] = "missing_or_invalid_both"

        return status

    def _build_duration_status(
        self,
        durations: pd.Series,
    ) -> pd.Series:
        """Classe les durées de livraison."""

        status = pd.Series(
            "normal",
            index=durations.index,
            dtype="string",
        )

        status.loc[durations.isna()] = "missing"

        status.loc[
            durations.notna()
            & durations.le(0)
        ] = "zero_or_negative"

        status.loc[
            durations.notna()
            & durations.gt(
                self.long_duration_minutes
            )
        ] = "long"

        return status

    def transform(
        self,
        dataframe: pd.DataFrame,
        processed_at: datetime | None = None,
    ) -> pd.DataFrame:
        """Transforme le DataFrame Bronze vers Silver."""

        self._validate_input(dataframe)

        transformed = dataframe.copy()

        integer_columns = (
            "order_id",
            "region_id",
            "courier_id",
            "aoi_id",
            "aoi_type",
            "_source_row_number",
        )

        for column_name in integer_columns:
            transformed[column_name] = pd.to_numeric(
                transformed[column_name],
                errors="raise",
            ).astype("Int64")

        transformed["city"] = (
            transformed["city"]
            .astype("string")
            .str.strip()
        )

        transformed["accept_timestamp"] = pd.to_datetime(
            transformed["accept_timestamp"],
            errors="coerce",
            utc=True,
        )

        transformed["delivery_timestamp"] = pd.to_datetime(
            transformed["delivery_timestamp"],
            errors="coerce",
            utc=True,
        )

        transformed["partition_timestamp"] = pd.to_datetime(
            "2000"
            + transformed["ds"].astype("string"),
            format="%Y%m%d",
            errors="coerce",
            utc=True,
        )

        (
            transformed["accept_gps_lng_clean"],
            transformed["accept_gps_lat_clean"],
            transformed["accept_gps_valid"],
        ) = self._clean_coordinate_pair(
            longitude=transformed["accept_gps_lng"],
            latitude=transformed["accept_gps_lat"],
        )

        (
            transformed["delivery_gps_lng_clean"],
            transformed["delivery_gps_lat_clean"],
            transformed["delivery_gps_valid"],
        ) = self._clean_coordinate_pair(
            longitude=transformed["delivery_gps_lng"],
            latitude=transformed["delivery_gps_lat"],
        )

        transformed["gps_quality_status"] = (
            self._build_gps_status(
                accept_gps_valid=(
                    transformed["accept_gps_valid"]
                ),
                delivery_gps_valid=(
                    transformed["delivery_gps_valid"]
                ),
            )
        )

        durations = pd.to_numeric(
            transformed["delivery_duration_minutes"],
            errors="coerce",
        ).astype("Float64")

        transformed["delivery_duration_minutes"] = durations

        transformed["delivery_duration_hours"] = (
            durations / 60
        ).round(3)

        transformed["delivery_duration_status"] = (
            self._build_duration_status(durations)
        )

        transformed["is_zero_or_negative_duration"] = (
            transformed["delivery_duration_status"]
            .eq("zero_or_negative")
        )

        transformed["is_long_delivery"] = (
            transformed["delivery_duration_status"]
            .eq("long")
        )

        transformed["accept_hour"] = (
            transformed["accept_timestamp"]
            .dt.hour
            .astype("Int8")
        )

        transformed["delivery_hour"] = (
            transformed["delivery_timestamp"]
            .dt.hour
            .astype("Int8")
        )

        transformed["is_quality_warning"] = (
            transformed["_quality_status"]
            .astype("string")
            .eq("warning")
        )

        silver_timestamp = (
            self._normalize_utc_timestamp(
                processed_at
            )
        )

        transformed["_silver_processed_at"] = (
            silver_timestamp
        )

        transformed["_silver_version"] = (
            SILVER_VERSION
        )

        logger.info(
            "lade_silver_transformation_completed",
            row_count=int(len(transformed)),
            column_count=int(
                len(transformed.columns)
            ),
            quality_warning_row_count=int(
                transformed["is_quality_warning"]
                .sum()
            ),
            gps_issue_row_count=int(
                transformed["gps_quality_status"]
                .ne("complete")
                .sum()
            ),
            duration_issue_row_count=int(
                transformed[
                    "delivery_duration_status"
                ]
                .ne("normal")
                .sum()
            ),
        )

        return transformed

    def write_parquet(
        self,
        dataframe: pd.DataFrame,
        output_path: Path,
    ) -> Path:
        """Écrit atomiquement le fichier Silver en Parquet."""

        resolved_output_path = output_path.resolve()

        resolved_output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        temporary_path = (
            resolved_output_path.with_name(
                f"{resolved_output_path.stem}"
                f".tmp{resolved_output_path.suffix}"
            )
        )

        logger.info(
            "lade_silver_parquet_write_started",
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

            with temporary_path.open(
                "rb"
            ) as parquet_stream:
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
                raise LaDeSilverBuildError(
                    "Le nombre de lignes du fichier Silver "
                    "ne correspond pas au DataFrame."
                )

            missing_columns = set(
                dataframe.columns
            ).difference(parquet_columns)

            if missing_columns:
                missing_text = ", ".join(
                    sorted(missing_columns)
                )

                raise LaDeSilverBuildError(
                    "Des colonnes sont absentes du Parquet "
                    f"Silver : {missing_text}"
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
                    "lade_silver_temporary_cleanup_failed",
                    temporary_path=str(
                        temporary_path
                    ),
                )

            if isinstance(
                error,
                LaDeSilverBuildError,
            ):
                raise

            logger.exception(
                "lade_silver_parquet_write_failed",
                output_path=str(
                    resolved_output_path
                ),
                error_type=type(error).__name__,
            )

            raise LaDeSilverBuildError(
                "Impossible d'écrire le fichier Silver : "
                f"{resolved_output_path}"
            ) from error

        logger.info(
            "lade_silver_parquet_write_completed",
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
        source_path: Path,
        output_path: Path,
        processed_at: datetime | None = None,
    ) -> LaDeSilverResult:
        """Transforme puis écrit le fichier Silver."""

        created_at = (
            processed_at or datetime.now(UTC)
        )

        transformed = self.transform(
            dataframe=dataframe,
            processed_at=created_at,
        )

        written_path = self.write_parquet(
            dataframe=transformed,
            output_path=output_path,
        )

        return LaDeSilverResult(
            source_path=str(source_path.resolve()),
            output_path=str(written_path),
            created_at=created_at,
            silver_version=SILVER_VERSION,
            row_count=int(len(transformed)),
            column_count=int(
                len(transformed.columns)
            ),
            quality_warning_row_count=int(
                transformed["is_quality_warning"]
                .sum()
            ),
            gps_issue_row_count=int(
                transformed["gps_quality_status"]
                .ne("complete")
                .sum()
            ),
            duration_issue_row_count=int(
                transformed[
                    "delivery_duration_status"
                ]
                .ne("normal")
                .sum()
            ),
            compression=self.compression,
            parquet_size_bytes=(
                written_path.stat().st_size
            ),
        )