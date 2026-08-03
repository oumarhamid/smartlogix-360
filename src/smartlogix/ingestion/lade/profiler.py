from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from smartlogix.common import get_logger

logger = get_logger(__name__)


class LaDeCsvProfilingError(RuntimeError):
    """Erreur rencontrée pendant le profilage d'un fichier LaDe."""


@dataclass(frozen=True, slots=True)
class LaDeColumnProfile:
    """Statistiques descriptives d'une colonne CSV."""

    name: str
    dtype: str
    row_count: int
    non_null_count: int
    null_count: int
    null_percentage: float
    unique_count: int
    numeric_minimum: float | int | None
    numeric_maximum: float | int | None
    sample_values: tuple[Any, ...]

    def to_dict(self) -> dict[str, Any]:
        """Convertit le profil de colonne en dictionnaire."""

        data = asdict(self)
        data["sample_values"] = list(self.sample_values)

        return data


@dataclass(frozen=True, slots=True)
class LaDeCsvProfile:
    """Rapport complet de profilage d'un fichier CSV LaDe."""

    file_path: str
    file_size_bytes: int
    profiled_at: datetime
    row_count: int
    column_count: int
    duplicate_row_count: int
    expected_columns: tuple[str, ...]
    missing_columns: tuple[str, ...]
    unexpected_columns: tuple[str, ...]
    columns: tuple[LaDeColumnProfile, ...]
    coordinate_bounds: dict[str, dict[str, float | None]]
    delivery_duration_minutes: dict[str, float | int | None]

    def to_dict(self) -> dict[str, Any]:
        """Convertit le rapport en dictionnaire sérialisable."""

        return {
            "file_path": self.file_path,
            "file_size_bytes": self.file_size_bytes,
            "profiled_at": self.profiled_at.isoformat(),
            "row_count": self.row_count,
            "column_count": self.column_count,
            "duplicate_row_count": self.duplicate_row_count,
            "expected_columns": list(self.expected_columns),
            "missing_columns": list(self.missing_columns),
            "unexpected_columns": list(
                self.unexpected_columns
            ),
            "columns": [
                column.to_dict()
                for column in self.columns
            ],
            "coordinate_bounds": self.coordinate_bounds,
            "delivery_duration_minutes": (
                self.delivery_duration_minutes
            ),
        }


class LaDeCsvProfiler:
    """Analyse le schéma et la qualité d'un fichier CSV LaDe."""

    EXPECTED_DELIVERY_COLUMNS = (
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

    COORDINATE_COLUMNS = (
        "lng",
        "lat",
        "accept_gps_lng",
        "accept_gps_lat",
        "delivery_gps_lng",
        "delivery_gps_lat",
    )

    @staticmethod
    def _normalize_scalar(value: Any) -> Any:
        """Convertit une valeur pandas en valeur JSON standard."""

        if pd.isna(value):
            return None

        if hasattr(value, "item"):
            return value.item()

        return value

    @classmethod
    def _profile_column(
        cls,
        series: pd.Series,
    ) -> LaDeColumnProfile:
        """Produit les statistiques d'une colonne."""

        row_count = int(len(series))
        null_count = int(series.isna().sum())
        non_null_count = row_count - null_count

        null_percentage = (
            round((null_count / row_count) * 100, 3)
            if row_count
            else 0.0
        )

        numeric_minimum: float | int | None = None
        numeric_maximum: float | int | None = None

        if pd.api.types.is_numeric_dtype(series):
            numeric_series = pd.to_numeric(
                series,
                errors="coerce",
            ).dropna()

            if not numeric_series.empty:
                numeric_minimum = cls._normalize_scalar(
                    numeric_series.min()
                )
                numeric_maximum = cls._normalize_scalar(
                    numeric_series.max()
                )

        sample_values = tuple(
            cls._normalize_scalar(value)
            for value in series.dropna().drop_duplicates().head(5)
        )

        return LaDeColumnProfile(
            name=str(series.name),
            dtype=str(series.dtype),
            row_count=row_count,
            non_null_count=non_null_count,
            null_count=null_count,
            null_percentage=null_percentage,
            unique_count=int(series.nunique(dropna=True)),
            numeric_minimum=numeric_minimum,
            numeric_maximum=numeric_maximum,
            sample_values=sample_values,
        )

    @classmethod
    def _calculate_coordinate_bounds(
        cls,
        dataframe: pd.DataFrame,
    ) -> dict[str, dict[str, float | None]]:
        """Calcule les bornes des colonnes géographiques."""

        bounds: dict[str, dict[str, float | None]] = {}

        for column_name in cls.COORDINATE_COLUMNS:
            if column_name not in dataframe.columns:
                continue

            numeric_values = pd.to_numeric(
                dataframe[column_name],
                errors="coerce",
            ).dropna()

            minimum: float | None = None
            maximum: float | None = None

            if not numeric_values.empty:
                minimum = float(numeric_values.min())
                maximum = float(numeric_values.max())

            bounds[column_name] = {
                "minimum": minimum,
                "maximum": maximum,
            }

        return bounds

    @staticmethod
    def _calculate_delivery_durations(
        dataframe: pd.DataFrame,
    ) -> dict[str, float | int | None]:
        """Calcule les durées entre acceptation et livraison."""

        required_columns = {
            "accept_time",
            "delivery_time",
        }

        if not required_columns.issubset(dataframe.columns):
            return {
                "valid_count": 0,
                "missing_or_invalid_count": int(
                    len(dataframe)
                ),
                "minimum": None,
                "maximum": None,
                "mean": None,
                "median": None,
                "midnight_rollover_count": 0,
            }

        accept_times = pd.to_datetime(
            dataframe["accept_time"],
            format="%m-%d %H:%M:%S",
            errors="coerce",
        )

        delivery_times = pd.to_datetime(
            dataframe["delivery_time"],
            format="%m-%d %H:%M:%S",
            errors="coerce",
        )

        durations = delivery_times - accept_times
        midnight_mask = durations < pd.Timedelta(0)

        durations = durations.where(
            ~midnight_mask,
            durations + pd.Timedelta(days=1),
        )

        duration_minutes = (
            durations.dt.total_seconds() / 60
        )

        valid_durations = duration_minutes.dropna()

        if valid_durations.empty:
            return {
                "valid_count": 0,
                "missing_or_invalid_count": int(
                    len(dataframe)
                ),
                "minimum": None,
                "maximum": None,
                "mean": None,
                "median": None,
                "midnight_rollover_count": int(
                    midnight_mask.sum()
                ),
            }

        return {
            "valid_count": int(valid_durations.count()),
            "missing_or_invalid_count": int(
                len(dataframe) - valid_durations.count()
            ),
            "minimum": round(
                float(valid_durations.min()),
                3,
            ),
            "maximum": round(
                float(valid_durations.max()),
                3,
            ),
            "mean": round(
                float(valid_durations.mean()),
                3,
            ),
            "median": round(
                float(valid_durations.median()),
                3,
            ),
            "midnight_rollover_count": int(
                midnight_mask.sum()
            ),
        }

    def profile(
        self,
        file_path: Path,
    ) -> LaDeCsvProfile:
        """Charge et profile un fichier CSV LaDe."""

        resolved_path = file_path.resolve()

        if not resolved_path.is_file():
            raise LaDeCsvProfilingError(
                f"Le fichier CSV est introuvable : {resolved_path}"
            )

        logger.info(
            "lade_csv_profiling_started",
            file_path=str(resolved_path),
        )

        try:
            dataframe = pd.read_csv(
                resolved_path,
                low_memory=False,
                na_values=[
                    "nan",
                    "NaN",
                    "NAN",
                ],
                keep_default_na=True,
            )
        except Exception as error:
            logger.exception(
                "lade_csv_profiling_failed",
                file_path=str(resolved_path),
                error_type=type(error).__name__,
            )

            raise LaDeCsvProfilingError(
                f"Impossible de lire le CSV : {resolved_path}"
            ) from error

        actual_columns = tuple(
            str(column)
            for column in dataframe.columns
        )

        expected_columns = self.EXPECTED_DELIVERY_COLUMNS

        missing_columns = tuple(
            column
            for column in expected_columns
            if column not in actual_columns
        )

        unexpected_columns = tuple(
            column
            for column in actual_columns
            if column not in expected_columns
        )

        profile = LaDeCsvProfile(
            file_path=str(resolved_path),
            file_size_bytes=resolved_path.stat().st_size,
            profiled_at=datetime.now(UTC),
            row_count=int(len(dataframe)),
            column_count=int(len(dataframe.columns)),
            duplicate_row_count=int(
                dataframe.duplicated().sum()
            ),
            expected_columns=expected_columns,
            missing_columns=missing_columns,
            unexpected_columns=unexpected_columns,
            columns=tuple(
                self._profile_column(dataframe[column])
                for column in dataframe.columns
            ),
            coordinate_bounds=(
                self._calculate_coordinate_bounds(dataframe)
            ),
            delivery_duration_minutes=(
                self._calculate_delivery_durations(dataframe)
            ),
        )

        logger.info(
            "lade_csv_profiling_completed",
            file_path=str(resolved_path),
            row_count=profile.row_count,
            column_count=profile.column_count,
            duplicate_row_count=(
                profile.duplicate_row_count
            ),
            missing_columns=profile.missing_columns,
            unexpected_columns=profile.unexpected_columns,
        )

        return profile

    @staticmethod
    def write_report(
        profile: LaDeCsvProfile,
        output_path: Path,
    ) -> Path:
        """Écrit atomiquement le rapport JSON de profilage."""

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        temporary_path = output_path.with_name(
            f"{output_path.name}.tmp"
        )

        content = json.dumps(
            profile.to_dict(),
            ensure_ascii=False,
            indent=2,
        )

        temporary_path.write_text(
            content,
            encoding="utf-8",
        )

        temporary_path.replace(output_path)

        logger.info(
            "lade_csv_profile_written",
            output_path=str(output_path),
            row_count=profile.row_count,
        )

        return output_path