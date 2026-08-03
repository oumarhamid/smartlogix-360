from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from smartlogix.common import get_logger

logger = get_logger(__name__)

GOLD_VERSION = "1.0"

GOLD_REQUIRED_COLUMNS = (
    "order_id",
    "region_id",
    "city",
    "courier_id",
    "aoi_id",
    "aoi_type",
    "partition_timestamp",
    "accept_timestamp",
    "delivery_timestamp",
    "delivery_duration_minutes",
    "delivery_duration_status",
    "accept_gps_lng_clean",
    "accept_gps_lat_clean",
    "delivery_gps_lng_clean",
    "delivery_gps_lat_clean",
    "accept_gps_valid",
    "delivery_gps_valid",
    "gps_quality_status",
    "is_quality_warning",
    "_quality_warning_count",
    "_source_file",
    "_source_sha256",
    "_dataset_revision",
    "_silver_processed_at",
    "_silver_version",
)

DELIVERY_FACT_COLUMNS = (
    "order_id",
    "delivery_date",
    "delivery_year",
    "delivery_month",
    "delivery_day",
    "delivery_weekday",
    "is_weekend",
    "region_id",
    "city",
    "courier_id",
    "aoi_id",
    "aoi_type",
    "accept_timestamp",
    "delivery_timestamp",
    "accept_hour",
    "delivery_hour",
    "delivery_duration_minutes",
    "delivery_duration_hours",
    "kpi_duration_minutes",
    "delivery_duration_status",
    "is_valid_duration",
    "is_within_sla",
    "is_late_delivery",
    "sla_minutes",
    "accept_gps_lng",
    "accept_gps_lat",
    "delivery_gps_lng",
    "delivery_gps_lat",
    "accept_gps_valid",
    "delivery_gps_valid",
    "has_complete_gps",
    "gps_quality_status",
    "is_quality_warning",
    "quality_warning_count",
    "delivery_count",
    "source_file",
    "source_sha256",
    "dataset_revision",
    "source_silver_version",
    "_gold_processed_at",
    "_gold_version",
)


class LaDeGoldBuildError(RuntimeError):
    """Erreur rencontrée pendant la création de la couche Gold."""


@dataclass(frozen=True, slots=True)
class LaDeGoldTables:
    """Ensemble des tables analytiques Gold."""

    delivery_fact: pd.DataFrame
    courier_daily_performance: pd.DataFrame
    city_daily_performance: pd.DataFrame


@dataclass(frozen=True, slots=True)
class LaDeGoldTableResult:
    """Résumé d'une table Gold écrite sur disque."""

    table_name: str
    output_path: str
    row_count: int
    column_count: int
    parquet_size_bytes: int


@dataclass(frozen=True, slots=True)
class LaDeGoldResult:
    """Résumé global de la production Gold."""

    source_path: str
    output_directory: str
    created_at: datetime
    gold_version: str
    sla_minutes: float
    compression: str
    delivery_fact: LaDeGoldTableResult
    courier_daily_performance: LaDeGoldTableResult
    city_daily_performance: LaDeGoldTableResult

    def to_dict(self) -> dict[str, Any]:
        """Convertit le résultat en dictionnaire sérialisable."""

        data = asdict(self)
        data["created_at"] = self.created_at.isoformat()

        return data


class LaDeGoldBuilder:
    """Construit les tables analytiques Gold de LaDe-D."""

    def __init__(
        self,
        sla_minutes: float = 240.0,
        compression: str = "zstd",
    ) -> None:
        if sla_minutes <= 0:
            raise ValueError(
                "sla_minutes doit être strictement positif."
            )

        self.sla_minutes = float(sla_minutes)
        self.compression = compression

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
        """Vérifie que le DataFrame Silver est exploitable."""

        missing_columns = set(
            GOLD_REQUIRED_COLUMNS
        ).difference(dataframe.columns)

        if missing_columns:
            missing_text = ", ".join(
                sorted(missing_columns)
            )

            raise LaDeGoldBuildError(
                "Des colonnes Silver obligatoires sont absentes : "
                f"{missing_text}"
            )

        if dataframe.empty:
            raise LaDeGoldBuildError(
                "Le DataFrame Silver est vide."
            )

        if dataframe["order_id"].isna().any():
            raise LaDeGoldBuildError(
                "La colonne order_id contient des valeurs nulles."
            )

        if dataframe["order_id"].duplicated().any():
            raise LaDeGoldBuildError(
                "La colonne order_id contient des doublons."
            )

    @staticmethod
    def _safe_rate(
        numerator: pd.Series,
        denominator: pd.Series,
    ) -> pd.Series:
        """Calcule un taux en pourcentage sans division par zéro."""

        numeric_numerator = pd.to_numeric(
            numerator,
            errors="coerce",
        ).fillna(0)

        numeric_denominator = pd.to_numeric(
            denominator,
            errors="coerce",
        ).fillna(0)

        rate = pd.Series(
            0.0,
            index=numeric_denominator.index,
            dtype="Float64",
        )

        valid_mask = numeric_denominator.gt(0)

        rate.loc[valid_mask] = (
            numeric_numerator.loc[valid_mask]
            / numeric_denominator.loc[valid_mask]
            * 100
        )

        return rate.round(2)

    def _build_delivery_fact(
        self,
        dataframe: pd.DataFrame,
        processed_at: datetime | None,
    ) -> pd.DataFrame:
        """Construit la table de faits des livraisons."""

        fact = dataframe.copy()

        identifier_columns = (
            "order_id",
            "region_id",
            "courier_id",
            "aoi_id",
            "aoi_type",
        )

        for column_name in identifier_columns:
            fact[column_name] = pd.to_numeric(
                fact[column_name],
                errors="raise",
            ).astype("Int64")

        fact["city"] = (
            fact["city"]
            .astype("string")
            .str.strip()
        )

        timestamp_columns = (
            "partition_timestamp",
            "accept_timestamp",
            "delivery_timestamp",
        )

        for column_name in timestamp_columns:
            fact[column_name] = pd.to_datetime(
                fact[column_name],
                errors="coerce",
                utc=True,
            )

        if fact["partition_timestamp"].isna().any():
            raise LaDeGoldBuildError(
                "La date de partition contient des valeurs invalides."
            )

        if fact["accept_timestamp"].isna().any():
            raise LaDeGoldBuildError(
                "La date d'acceptation contient des valeurs invalides."
            )

        if fact["delivery_timestamp"].isna().any():
            raise LaDeGoldBuildError(
                "La date de livraison contient des valeurs invalides."
            )

        fact["delivery_date"] = (
            fact["partition_timestamp"]
            .dt.normalize()
        )

        fact["delivery_year"] = (
            fact["delivery_date"]
            .dt.year
            .astype("Int16")
        )

        fact["delivery_month"] = (
            fact["delivery_date"]
            .dt.month
            .astype("Int8")
        )

        fact["delivery_day"] = (
            fact["delivery_date"]
            .dt.day
            .astype("Int8")
        )

        fact["delivery_weekday"] = (
            fact["delivery_date"]
            .dt.dayofweek
            .astype("Int8")
        )

        fact["is_weekend"] = (
            fact["delivery_weekday"]
            .ge(5)
        )

        fact["accept_hour"] = (
            fact["accept_timestamp"]
            .dt.hour
            .astype("Int8")
        )

        fact["delivery_hour"] = (
            fact["delivery_timestamp"]
            .dt.hour
            .astype("Int8")
        )

        durations = pd.to_numeric(
            fact["delivery_duration_minutes"],
            errors="coerce",
        ).astype("Float64")

        fact["delivery_duration_minutes"] = durations

        fact["delivery_duration_hours"] = (
            durations / 60
        ).round(3)

        fact["is_valid_duration"] = (
            fact["delivery_duration_status"]
            .astype("string")
            .eq("normal")
            & durations.notna()
            & durations.gt(0)
        )

        fact["kpi_duration_minutes"] = (
            durations.where(
                fact["is_valid_duration"]
            )
        )

        fact["sla_minutes"] = pd.Series(
            self.sla_minutes,
            index=fact.index,
            dtype="Float64",
        )

        fact["is_within_sla"] = (
            fact["is_valid_duration"]
            & durations.le(self.sla_minutes)
        )

        fact["is_late_delivery"] = (
            fact["is_valid_duration"]
            & durations.gt(self.sla_minutes)
        )

        coordinate_mapping = {
            "accept_gps_lng_clean": "accept_gps_lng",
            "accept_gps_lat_clean": "accept_gps_lat",
            "delivery_gps_lng_clean": "delivery_gps_lng",
            "delivery_gps_lat_clean": "delivery_gps_lat",
        }

        for source_column, target_column in (
            coordinate_mapping.items()
        ):
            fact[target_column] = pd.to_numeric(
                fact[source_column],
                errors="coerce",
            ).astype("Float64")

        fact["accept_gps_valid"] = (
            fact["accept_gps_valid"]
            .fillna(False)
            .astype(bool)
        )

        fact["delivery_gps_valid"] = (
            fact["delivery_gps_valid"]
            .fillna(False)
            .astype(bool)
        )

        fact["has_complete_gps"] = (
            fact["accept_gps_valid"]
            & fact["delivery_gps_valid"]
        )

        fact["gps_quality_status"] = (
            fact["gps_quality_status"]
            .astype("string")
        )

        fact["is_quality_warning"] = (
            fact["is_quality_warning"]
            .fillna(False)
            .astype(bool)
        )

        fact["quality_warning_count"] = (
            pd.to_numeric(
                fact["_quality_warning_count"],
                errors="coerce",
            )
            .fillna(0)
            .astype("Int16")
        )

        fact["delivery_count"] = pd.Series(
            1,
            index=fact.index,
            dtype="Int8",
        )

        fact["source_file"] = (
            fact["_source_file"]
            .astype("string")
        )

        fact["source_sha256"] = (
            fact["_source_sha256"]
            .astype("string")
        )

        fact["dataset_revision"] = (
            fact["_dataset_revision"]
            .astype("string")
        )

        fact["source_silver_version"] = (
            fact["_silver_version"]
            .astype("string")
        )

        fact["_gold_processed_at"] = (
            self._normalize_utc_timestamp(
                processed_at
            )
        )

        fact["_gold_version"] = GOLD_VERSION

        return fact.loc[
            :,
            list(DELIVERY_FACT_COLUMNS),
        ].sort_values(
            by=[
                "delivery_date",
                "city",
                "courier_id",
                "order_id",
            ],
            ignore_index=True,
        )

    def _build_courier_daily_performance(
        self,
        delivery_fact: pd.DataFrame,
    ) -> pd.DataFrame:
        """Agrège les indicateurs quotidiens par coursier."""

        grouped = (
            delivery_fact.groupby(
                [
                    "delivery_date",
                    "region_id",
                    "city",
                    "courier_id",
                ],
                as_index=False,
                dropna=False,
                observed=True,
                sort=True,
            )
            .agg(
                orders_total=(
                    "delivery_count",
                    "sum",
                ),
                orders_valid_duration=(
                    "is_valid_duration",
                    "sum",
                ),
                orders_within_sla=(
                    "is_within_sla",
                    "sum",
                ),
                orders_late=(
                    "is_late_delivery",
                    "sum",
                ),
                orders_quality_warning=(
                    "is_quality_warning",
                    "sum",
                ),
                orders_gps_complete=(
                    "has_complete_gps",
                    "sum",
                ),
                unique_aois=(
                    "aoi_id",
                    "nunique",
                ),
                avg_duration_minutes=(
                    "kpi_duration_minutes",
                    "mean",
                ),
                median_duration_minutes=(
                    "kpi_duration_minutes",
                    "median",
                ),
                min_duration_minutes=(
                    "kpi_duration_minutes",
                    "min",
                ),
                max_duration_minutes=(
                    "kpi_duration_minutes",
                    "max",
                ),
                first_accept_timestamp=(
                    "accept_timestamp",
                    "min",
                ),
                last_delivery_timestamp=(
                    "delivery_timestamp",
                    "max",
                ),
            )
        )

        count_columns = (
            "orders_total",
            "orders_valid_duration",
            "orders_within_sla",
            "orders_late",
            "orders_quality_warning",
            "orders_gps_complete",
            "unique_aois",
        )

        for column_name in count_columns:
            grouped[column_name] = (
                pd.to_numeric(
                    grouped[column_name],
                    errors="coerce",
                )
                .fillna(0)
                .astype("Int64")
            )

        duration_columns = (
            "avg_duration_minutes",
            "median_duration_minutes",
            "min_duration_minutes",
            "max_duration_minutes",
        )

        for column_name in duration_columns:
            grouped[column_name] = (
                pd.to_numeric(
                    grouped[column_name],
                    errors="coerce",
                )
                .astype("Float64")
                .round(3)
            )

        grouped["sla_compliance_rate"] = (
            self._safe_rate(
                numerator=grouped[
                    "orders_within_sla"
                ],
                denominator=grouped[
                    "orders_valid_duration"
                ],
            )
        )

        grouped["gps_completeness_rate"] = (
            self._safe_rate(
                numerator=grouped[
                    "orders_gps_complete"
                ],
                denominator=grouped[
                    "orders_total"
                ],
            )
        )

        grouped["quality_warning_rate"] = (
            self._safe_rate(
                numerator=grouped[
                    "orders_quality_warning"
                ],
                denominator=grouped[
                    "orders_total"
                ],
            )
        )

        grouped["sla_minutes"] = (
            self.sla_minutes
        )

        grouped["_gold_processed_at"] = (
            delivery_fact[
                "_gold_processed_at"
            ].iloc[0]
        )

        grouped["_gold_version"] = GOLD_VERSION

        return grouped

    def _build_city_daily_performance(
        self,
        delivery_fact: pd.DataFrame,
    ) -> pd.DataFrame:
        """Agrège les indicateurs quotidiens par ville."""

        grouped = (
            delivery_fact.groupby(
                [
                    "delivery_date",
                    "region_id",
                    "city",
                ],
                as_index=False,
                dropna=False,
                observed=True,
                sort=True,
            )
            .agg(
                orders_total=(
                    "delivery_count",
                    "sum",
                ),
                orders_valid_duration=(
                    "is_valid_duration",
                    "sum",
                ),
                orders_within_sla=(
                    "is_within_sla",
                    "sum",
                ),
                orders_late=(
                    "is_late_delivery",
                    "sum",
                ),
                orders_quality_warning=(
                    "is_quality_warning",
                    "sum",
                ),
                orders_gps_complete=(
                    "has_complete_gps",
                    "sum",
                ),
                unique_couriers=(
                    "courier_id",
                    "nunique",
                ),
                unique_aois=(
                    "aoi_id",
                    "nunique",
                ),
                avg_duration_minutes=(
                    "kpi_duration_minutes",
                    "mean",
                ),
                median_duration_minutes=(
                    "kpi_duration_minutes",
                    "median",
                ),
                min_duration_minutes=(
                    "kpi_duration_minutes",
                    "min",
                ),
                max_duration_minutes=(
                    "kpi_duration_minutes",
                    "max",
                ),
                first_accept_timestamp=(
                    "accept_timestamp",
                    "min",
                ),
                last_delivery_timestamp=(
                    "delivery_timestamp",
                    "max",
                ),
            )
        )

        count_columns = (
            "orders_total",
            "orders_valid_duration",
            "orders_within_sla",
            "orders_late",
            "orders_quality_warning",
            "orders_gps_complete",
            "unique_couriers",
            "unique_aois",
        )

        for column_name in count_columns:
            grouped[column_name] = (
                pd.to_numeric(
                    grouped[column_name],
                    errors="coerce",
                )
                .fillna(0)
                .astype("Int64")
            )

        duration_columns = (
            "avg_duration_minutes",
            "median_duration_minutes",
            "min_duration_minutes",
            "max_duration_minutes",
        )

        for column_name in duration_columns:
            grouped[column_name] = (
                pd.to_numeric(
                    grouped[column_name],
                    errors="coerce",
                )
                .astype("Float64")
                .round(3)
            )

        grouped["sla_compliance_rate"] = (
            self._safe_rate(
                numerator=grouped[
                    "orders_within_sla"
                ],
                denominator=grouped[
                    "orders_valid_duration"
                ],
            )
        )

        grouped["gps_completeness_rate"] = (
            self._safe_rate(
                numerator=grouped[
                    "orders_gps_complete"
                ],
                denominator=grouped[
                    "orders_total"
                ],
            )
        )

        grouped["quality_warning_rate"] = (
            self._safe_rate(
                numerator=grouped[
                    "orders_quality_warning"
                ],
                denominator=grouped[
                    "orders_total"
                ],
            )
        )

        grouped["sla_minutes"] = (
            self.sla_minutes
        )

        grouped["_gold_processed_at"] = (
            delivery_fact[
                "_gold_processed_at"
            ].iloc[0]
        )

        grouped["_gold_version"] = GOLD_VERSION

        return grouped

    def transform(
        self,
        dataframe: pd.DataFrame,
        processed_at: datetime | None = None,
    ) -> LaDeGoldTables:
        """Transforme le DataFrame Silver en tables Gold."""

        self._validate_input(dataframe)

        delivery_fact = (
            self._build_delivery_fact(
                dataframe=dataframe,
                processed_at=processed_at,
            )
        )

        courier_daily_performance = (
            self._build_courier_daily_performance(
                delivery_fact
            )
        )

        city_daily_performance = (
            self._build_city_daily_performance(
                delivery_fact
            )
        )

        logger.info(
            "lade_gold_transformation_completed",
            delivery_fact_rows=int(
                len(delivery_fact)
            ),
            courier_daily_rows=int(
                len(courier_daily_performance)
            ),
            city_daily_rows=int(
                len(city_daily_performance)
            ),
            sla_minutes=self.sla_minutes,
        )

        return LaDeGoldTables(
            delivery_fact=delivery_fact,
            courier_daily_performance=(
                courier_daily_performance
            ),
            city_daily_performance=(
                city_daily_performance
            ),
        )

    def _write_parquet(
        self,
        dataframe: pd.DataFrame,
        output_path: Path,
        table_name: str,
    ) -> LaDeGoldTableResult:
        """Écrit atomiquement une table Gold en Parquet."""

        resolved_output_path = (
            output_path.resolve()
        )

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
            "lade_gold_parquet_write_started",
            table_name=table_name,
            output_path=str(
                resolved_output_path
            ),
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
                raise LaDeGoldBuildError(
                    f"La table {table_name} ne contient pas "
                    "le nombre de lignes attendu."
                )

            missing_columns = set(
                dataframe.columns
            ).difference(parquet_columns)

            if missing_columns:
                missing_text = ", ".join(
                    sorted(missing_columns)
                )

                raise LaDeGoldBuildError(
                    f"Des colonnes sont absentes de "
                    f"{table_name} : {missing_text}"
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
                    "lade_gold_temporary_cleanup_failed",
                    table_name=table_name,
                    temporary_path=str(
                        temporary_path
                    ),
                )

            if isinstance(
                error,
                LaDeGoldBuildError,
            ):
                raise

            logger.exception(
                "lade_gold_parquet_write_failed",
                table_name=table_name,
                output_path=str(
                    resolved_output_path
                ),
                error_type=type(error).__name__,
            )

            raise LaDeGoldBuildError(
                f"Impossible d'écrire la table Gold "
                f"{table_name}."
            ) from error

        logger.info(
            "lade_gold_parquet_write_completed",
            table_name=table_name,
            output_path=str(
                resolved_output_path
            ),
            row_count=int(len(dataframe)),
            parquet_size_bytes=(
                resolved_output_path.stat().st_size
            ),
        )

        return LaDeGoldTableResult(
            table_name=table_name,
            output_path=str(
                resolved_output_path
            ),
            row_count=int(len(dataframe)),
            column_count=int(
                len(dataframe.columns)
            ),
            parquet_size_bytes=(
                resolved_output_path.stat().st_size
            ),
        )

    def write_tables(
        self,
        tables: LaDeGoldTables,
        output_directory: Path,
    ) -> tuple[
        LaDeGoldTableResult,
        LaDeGoldTableResult,
        LaDeGoldTableResult,
    ]:
        """Écrit les trois tables analytiques Gold."""

        resolved_directory = (
            output_directory.resolve()
        )

        delivery_fact_result = (
            self._write_parquet(
                dataframe=tables.delivery_fact,
                output_path=(
                    resolved_directory
                    / "delivery_fact.parquet"
                ),
                table_name="delivery_fact",
            )
        )

        courier_daily_result = (
            self._write_parquet(
                dataframe=(
                    tables.courier_daily_performance
                ),
                output_path=(
                    resolved_directory
                    / (
                        "courier_daily_"
                        "performance.parquet"
                    )
                ),
                table_name=(
                    "courier_daily_performance"
                ),
            )
        )

        city_daily_result = (
            self._write_parquet(
                dataframe=(
                    tables.city_daily_performance
                ),
                output_path=(
                    resolved_directory
                    / (
                        "city_daily_"
                        "performance.parquet"
                    )
                ),
                table_name=(
                    "city_daily_performance"
                ),
            )
        )

        return (
            delivery_fact_result,
            courier_daily_result,
            city_daily_result,
        )

    def build(
        self,
        dataframe: pd.DataFrame,
        source_path: Path,
        output_directory: Path,
        processed_at: datetime | None = None,
    ) -> LaDeGoldResult:
        """Transforme Silver puis écrit toutes les tables Gold."""

        created_at = (
            processed_at or datetime.now(UTC)
        )

        tables = self.transform(
            dataframe=dataframe,
            processed_at=created_at,
        )

        (
            delivery_fact_result,
            courier_daily_result,
            city_daily_result,
        ) = self.write_tables(
            tables=tables,
            output_directory=output_directory,
        )

        return LaDeGoldResult(
            source_path=str(source_path.resolve()),
            output_directory=str(
                output_directory.resolve()
            ),
            created_at=created_at,
            gold_version=GOLD_VERSION,
            sla_minutes=self.sla_minutes,
            compression=self.compression,
            delivery_fact=delivery_fact_result,
            courier_daily_performance=(
                courier_daily_result
            ),
            city_daily_performance=(
                city_daily_result
            ),
        )