from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaErrors

from smartlogix.common import get_logger

logger = get_logger(__name__)

DELIVERY_COLUMNS = (
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
)

STRING_COLUMNS = (
    "city",
    "accept_time",
    "accept_gps_time",
    "delivery_time",
    "delivery_gps_time",
    "ds",
)

MONTH_DAY_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DAY_PARTITION_FORMAT = "%Y%m%d"


def _parse_month_day_time(series: pd.Series) -> pd.Series:
    """Analyse une date LaDe avec une année fictive bissextile."""

    normalized = "2000-" + series.astype("string")

    return pd.to_datetime(
        normalized,
        format=MONTH_DAY_TIME_FORMAT,
        errors="coerce",
    )


def _valid_month_day_time(series: pd.Series) -> pd.Series:
    """Vérifie le format MM-DD HH:MM:SS."""

    return _parse_month_day_time(series).notna()


def _valid_day_partition(series: pd.Series) -> pd.Series:
    """Vérifie une partition LaDe au format MMDD."""

    normalized = "2000" + series.astype("string")

    parsed = pd.to_datetime(
        normalized,
        format=DAY_PARTITION_FORMAT,
        errors="coerce",
    )

    return parsed.notna()


def _accept_gps_pair_is_consistent(
    dataframe: pd.DataFrame,
) -> pd.Series:
    """Vérifie que les coordonnées GPS sont absentes ensemble."""

    return dataframe["accept_gps_lng"].isna().eq(
        dataframe["accept_gps_lat"].isna()
    )


def build_lade_delivery_schema() -> pa.DataFrameSchema:
    """Construit le contrat bloquant des livraisons LaDe."""

    positive_integer = pa.Check(
        lambda series: series > 0,
        name="positive_integer",
    )

    non_negative_integer = pa.Check(
        lambda series: series >= 0,
        name="non_negative_integer",
    )

    longitude_check = pa.Check(
        lambda series: series.between(-180, 180),
        name="valid_longitude",
    )

    latitude_check = pa.Check(
        lambda series: series.between(-90, 90),
        name="valid_latitude",
    )

    non_empty_string = pa.Check(
        lambda series: (
            series.astype("string")
            .str.strip()
            .str.len()
            .gt(0)
        ),
        name="non_empty_string",
    )

    valid_time = pa.Check(
        _valid_month_day_time,
        name="valid_month_day_time",
    )

    return pa.DataFrameSchema(
        columns={
            "order_id": pa.Column(
                pa.Int64,
                checks=positive_integer,
                nullable=False,
                coerce=True,
            ),
            "region_id": pa.Column(
                pa.Int64,
                checks=positive_integer,
                nullable=False,
                coerce=True,
            ),
            "city": pa.Column(
                pa.String,
                checks=non_empty_string,
                nullable=False,
                coerce=True,
            ),
            "courier_id": pa.Column(
                pa.Int64,
                checks=positive_integer,
                nullable=False,
                coerce=True,
            ),
            "lng": pa.Column(
                pa.Float64,
                checks=longitude_check,
                nullable=False,
                coerce=True,
            ),
            "lat": pa.Column(
                pa.Float64,
                checks=latitude_check,
                nullable=False,
                coerce=True,
            ),
            "aoi_id": pa.Column(
                pa.Int64,
                checks=non_negative_integer,
                nullable=False,
                coerce=True,
            ),
            "aoi_type": pa.Column(
                pa.Int64,
                checks=non_negative_integer,
                nullable=False,
                coerce=True,
            ),
            "accept_time": pa.Column(
                pa.String,
                checks=valid_time,
                nullable=False,
                coerce=True,
            ),
            "accept_gps_time": pa.Column(
                pa.String,
                checks=valid_time,
                nullable=False,
                coerce=True,
            ),
            "accept_gps_lng": pa.Column(
                pa.Float64,
                checks=longitude_check,
                nullable=True,
                coerce=True,
            ),
            "accept_gps_lat": pa.Column(
                pa.Float64,
                checks=latitude_check,
                nullable=True,
                coerce=True,
            ),
            "delivery_time": pa.Column(
                pa.String,
                checks=valid_time,
                nullable=False,
                coerce=True,
            ),
            "delivery_gps_time": pa.Column(
                pa.String,
                checks=valid_time,
                nullable=False,
                coerce=True,
            ),
            "delivery_gps_lng": pa.Column(
                pa.Float64,
                checks=longitude_check,
                nullable=False,
                coerce=True,
            ),
            "delivery_gps_lat": pa.Column(
                pa.Float64,
                checks=latitude_check,
                nullable=False,
                coerce=True,
            ),
            "ds": pa.Column(
                pa.String,
                checks=pa.Check(
                    _valid_day_partition,
                    name="valid_day_partition",
                ),
                nullable=False,
                coerce=True,
            ),
        },
        checks=[
            pa.Check(
                _accept_gps_pair_is_consistent,
                name="accept_gps_pair_consistent",
            )
        ],
        strict=True,
        ordered=True,
        unique=["order_id"],
        report_duplicates="all",
        unique_column_names=True,
        name="lade_delivery_contract",
        title="Contrat de qualité LaDe-D",
        description=(
            "Contrat bloquant des fichiers de livraison LaDe."
        ),
    )


def read_lade_delivery_csv(
    file_path: Path,
) -> pd.DataFrame:
    """Charge un CSV LaDe en préservant les colonnes textuelles."""

    resolved_path = file_path.resolve()

    if not resolved_path.is_file():
        raise FileNotFoundError(
            f"Le fichier LaDe est introuvable : {resolved_path}"
        )

    return pd.read_csv(
        resolved_path,
        low_memory=False,
        dtype={
            column: "string"
            for column in STRING_COLUMNS
        },
        na_values=[
            "nan",
            "NaN",
            "NAN",
        ],
        keep_default_na=True,
    )


@dataclass(frozen=True, slots=True)
class LaDeQualityWarning:
    """Avertissement métier non bloquant."""

    rule: str
    description: str
    columns: tuple[str, ...]
    row_count: int
    row_indices: tuple[Any, ...]
    sample_records: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        """Convertit l'avertissement en dictionnaire."""

        data = asdict(self)
        data["columns"] = list(self.columns)
        data["row_indices"] = list(self.row_indices)
        data["sample_records"] = list(
            self.sample_records
        )

        return data


@dataclass(frozen=True, slots=True)
class LaDeQualityReport:
    """Résultat complet de la validation LaDe."""

    source_path: str | None
    validated_at: datetime
    row_count: int
    blocking_passed: bool
    blocking_failure_count: int
    blocking_failure_cases: tuple[
        dict[str, Any],
        ...,
    ]
    warning_rule_count: int
    warning_row_count: int
    warnings: tuple[LaDeQualityWarning, ...]

    def to_dict(self) -> dict[str, Any]:
        """Convertit le rapport en dictionnaire JSON."""

        return {
            "source_path": self.source_path,
            "validated_at": self.validated_at.isoformat(),
            "row_count": self.row_count,
            "blocking_passed": self.blocking_passed,
            "blocking_failure_count": (
                self.blocking_failure_count
            ),
            "blocking_failure_cases": list(
                self.blocking_failure_cases
            ),
            "warning_rule_count": self.warning_rule_count,
            "warning_row_count": self.warning_row_count,
            "warnings": [
                warning.to_dict()
                for warning in self.warnings
            ],
        }


class LaDeDeliveryQualityValidator:
    """Valide le contrat Pandera et les anomalies métier."""

    def __init__(
        self,
        sample_limit: int = 20,
        long_duration_minutes: float = 24 * 60,
        near_zero_coordinate: float = 1.0,
    ) -> None:
        self.schema = build_lade_delivery_schema()
        self.sample_limit = sample_limit
        self.long_duration_minutes = (
            long_duration_minutes
        )
        self.near_zero_coordinate = (
            near_zero_coordinate
        )

    @staticmethod
    def _normalize_scalar(value: Any) -> Any:
        """Convertit une valeur pandas en valeur JSON."""

        if value is None or value is pd.NA:
            return None

        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass

        if isinstance(value, pd.Timestamp):
            return value.isoformat()

        if hasattr(value, "item"):
            return value.item()

        return value

    @classmethod
    def _normalize_records(
        cls,
        records: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], ...]:
        """Normalise une liste d'enregistrements."""

        return tuple(
            {
                key: cls._normalize_scalar(value)
                for key, value in record.items()
            }
            for record in records
        )

    @classmethod
    def _normalize_failure_cases(
        cls,
        failure_cases: pd.DataFrame,
        sample_limit: int,
    ) -> tuple[dict[str, Any], ...]:
        """Normalise un échantillon des erreurs Pandera."""

        if failure_cases.empty:
            return ()

        records = (
            failure_cases.head(sample_limit)
            .to_dict(orient="records")
        )

        return cls._normalize_records(records)

    @staticmethod
    def _calculate_duration_minutes(
        dataframe: pd.DataFrame,
    ) -> pd.Series:
        """Calcule la durée entre acceptation et livraison."""

        accept_times = _parse_month_day_time(
            dataframe["accept_time"]
        )

        delivery_times = _parse_month_day_time(
            dataframe["delivery_time"]
        )

        durations = (
            delivery_times - accept_times
        ).dt.total_seconds() / 60

        overnight_mask = durations < 0

        return durations.where(
            ~overnight_mask,
            durations + (24 * 60),
        )

    def _build_warning(
        self,
        dataframe: pd.DataFrame,
        mask: pd.Series,
        rule: str,
        description: str,
        columns: tuple[str, ...],
    ) -> LaDeQualityWarning | None:
        """Construit un avertissement à partir d'un masque."""

        normalized_mask = (
            mask.reindex(dataframe.index)
            .fillna(False)
            .astype(bool)
        )

        warning_rows = dataframe.loc[
            normalized_mask,
            list(columns),
        ]

        if warning_rows.empty:
            return None

        row_indices = tuple(
            self._normalize_scalar(index)
            for index in warning_rows.index.tolist()
        )

        samples = warning_rows.head(
            self.sample_limit
        ).copy()

        samples.insert(
            0,
            "row_index",
            samples.index,
        )

        sample_records = self._normalize_records(
            samples.to_dict(orient="records")
        )

        return LaDeQualityWarning(
            rule=rule,
            description=description,
            columns=columns,
            row_count=int(len(warning_rows)),
            row_indices=row_indices,
            sample_records=sample_records,
        )

    def _collect_warnings(
        self,
        dataframe: pd.DataFrame,
    ) -> tuple[LaDeQualityWarning, ...]:
        """Détecte les anomalies non bloquantes."""

        warnings: list[LaDeQualityWarning] = []

        required_accept_gps = {
            "accept_gps_lng",
            "accept_gps_lat",
        }

        if required_accept_gps.issubset(
            dataframe.columns
        ):
            missing_accept_gps = (
                dataframe["accept_gps_lng"].isna()
                & dataframe["accept_gps_lat"].isna()
            )

            warning = self._build_warning(
                dataframe=dataframe,
                mask=missing_accept_gps,
                rule="missing_accept_gps",
                description=(
                    "Les deux coordonnées GPS "
                    "d'acceptation sont absentes."
                ),
                columns=(
                    "order_id",
                    "accept_gps_lng",
                    "accept_gps_lat",
                ),
            )

            if warning is not None:
                warnings.append(warning)

            near_zero_accept_gps = (
                dataframe["accept_gps_lng"].notna()
                & dataframe["accept_gps_lat"].notna()
                & (
                    dataframe["accept_gps_lng"]
                    .abs()
                    .lt(self.near_zero_coordinate)
                    | dataframe["accept_gps_lat"]
                    .abs()
                    .lt(self.near_zero_coordinate)
                )
            )

            warning = self._build_warning(
                dataframe=dataframe,
                mask=near_zero_accept_gps,
                rule="near_zero_accept_gps",
                description=(
                    "Les coordonnées GPS d'acceptation "
                    "sont anormalement proches de zéro."
                ),
                columns=(
                    "order_id",
                    "accept_gps_lng",
                    "accept_gps_lat",
                ),
            )

            if warning is not None:
                warnings.append(warning)

        required_delivery_gps = {
            "delivery_gps_lng",
            "delivery_gps_lat",
        }

        if required_delivery_gps.issubset(
            dataframe.columns
        ):
            near_zero_delivery_gps = (
                dataframe["delivery_gps_lng"]
                .abs()
                .lt(self.near_zero_coordinate)
                | dataframe["delivery_gps_lat"]
                .abs()
                .lt(self.near_zero_coordinate)
            )

            warning = self._build_warning(
                dataframe=dataframe,
                mask=near_zero_delivery_gps,
                rule="near_zero_delivery_gps",
                description=(
                    "Les coordonnées GPS de livraison "
                    "sont anormalement proches de zéro."
                ),
                columns=(
                    "order_id",
                    "delivery_gps_lng",
                    "delivery_gps_lat",
                ),
            )

            if warning is not None:
                warnings.append(warning)

        required_times = {
            "accept_time",
            "delivery_time",
        }

        if required_times.issubset(dataframe.columns):
            durations = self._calculate_duration_minutes(
                dataframe
            )

            zero_or_negative_duration = (
                durations.notna()
                & durations.le(0)
            )

            warning = self._build_warning(
                dataframe=dataframe,
                mask=zero_or_negative_duration,
                rule="zero_or_negative_duration",
                description=(
                    "La durée calculée est nulle "
                    "ou négative."
                ),
                columns=(
                    "order_id",
                    "accept_time",
                    "delivery_time",
                ),
            )

            if warning is not None:
                warnings.append(warning)

            long_duration = (
                durations.notna()
                & durations.gt(
                    self.long_duration_minutes
                )
            )

            warning = self._build_warning(
                dataframe=dataframe,
                mask=long_duration,
                rule="long_delivery_duration",
                description=(
                    "La durée de livraison dépasse "
                    f"{self.long_duration_minutes} minutes."
                ),
                columns=(
                    "order_id",
                    "accept_time",
                    "delivery_time",
                ),
            )

            if warning is not None:
                warnings.append(warning)

        required_partition_columns = {
            "accept_time",
            "ds",
        }

        if required_partition_columns.issubset(
            dataframe.columns
        ):
            accept_time = dataframe[
                "accept_time"
            ].astype("string")

            expected_partition = (
                accept_time.str.slice(0, 2)
                + accept_time.str.slice(3, 5)
            )

            partition_mismatch = (
                dataframe["ds"]
                .astype("string")
                .ne(expected_partition)
            )

            warning = self._build_warning(
                dataframe=dataframe,
                mask=partition_mismatch,
                rule="partition_accept_date_mismatch",
                description=(
                    "La partition ds ne correspond pas "
                    "à la date d'acceptation."
                ),
                columns=(
                    "order_id",
                    "accept_time",
                    "ds",
                ),
            )

            if warning is not None:
                warnings.append(warning)

        return tuple(warnings)

    def validate(
        self,
        dataframe: pd.DataFrame,
        source_path: Path | None = None,
    ) -> LaDeQualityReport:
        """Exécute les règles bloquantes et les avertissements."""

        logger.info(
            "lade_quality_validation_started",
            source_path=(
                str(source_path)
                if source_path is not None
                else None
            ),
            row_count=int(len(dataframe)),
        )

        blocking_failure_count = 0
        blocking_failure_cases: tuple[
            dict[str, Any],
            ...,
        ] = ()

        try:
            self.schema.validate(
                dataframe.copy(),
                lazy=True,
            )
        except SchemaErrors as error:
            blocking_failure_count = int(
                len(error.failure_cases)
            )

            blocking_failure_cases = (
                self._normalize_failure_cases(
                    failure_cases=error.failure_cases,
                    sample_limit=self.sample_limit,
                )
            )

        warnings = self._collect_warnings(dataframe)

        warning_indices = {
            index
            for warning in warnings
            for index in warning.row_indices
        }

        report = LaDeQualityReport(
            source_path=(
                str(source_path.resolve())
                if source_path is not None
                else None
            ),
            validated_at=datetime.now(UTC),
            row_count=int(len(dataframe)),
            blocking_passed=(
                blocking_failure_count == 0
            ),
            blocking_failure_count=(
                blocking_failure_count
            ),
            blocking_failure_cases=(
                blocking_failure_cases
            ),
            warning_rule_count=len(warnings),
            warning_row_count=len(warning_indices),
            warnings=warnings,
        )

        logger.info(
            "lade_quality_validation_completed",
            source_path=report.source_path,
            row_count=report.row_count,
            blocking_passed=report.blocking_passed,
            blocking_failure_count=(
                report.blocking_failure_count
            ),
            warning_rule_count=(
                report.warning_rule_count
            ),
            warning_row_count=(
                report.warning_row_count
            ),
        )

        return report

    @staticmethod
    def write_report(
        report: LaDeQualityReport,
        output_path: Path,
    ) -> Path:
        """Écrit atomiquement le rapport de qualité."""

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        temporary_path = output_path.with_name(
            f"{output_path.name}.tmp"
        )

        content = json.dumps(
            report.to_dict(),
            ensure_ascii=False,
            indent=2,
        )

        temporary_path.write_text(
            content,
            encoding="utf-8",
        )

        temporary_path.replace(output_path)

        logger.info(
            "lade_quality_report_written",
            output_path=str(output_path),
            blocking_passed=report.blocking_passed,
        )

        return output_path