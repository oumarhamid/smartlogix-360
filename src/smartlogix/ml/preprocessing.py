from __future__ import annotations

from typing import Any

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import (
    FunctionTransformer,
    OneHotEncoder,
    StandardScaler,
)

from smartlogix.ml.dataset import TARGET_COLUMN
from smartlogix.ml.features import HISTORICAL_FEATURE_COLUMNS

# Les identifiants à très forte cardinalité sont volontairement exclus
# de la V1.
#
# courier_id et aoi_id ne sont pas one-hot encodés :
# - cardinalité élevée ;
# - coût mémoire inutile ;
# - risque de surapprentissage ;
# - les informations comportementales du coursier sont déjà représentées
#   par les features historiques J-1.
HIGH_CARDINALITY_COLUMNS = (
    "courier_id",
    "aoi_id",
)

CATEGORICAL_COLUMNS = (
    "region_id",
    "city",
    "aoi_type",
    "accept_period",
)

BASE_NUMERIC_COLUMNS = (
    "sla_minutes",
    "accept_gps_lng",
    "accept_gps_lat",
    "accept_hour",
    "accept_weekday",
    "accept_month",
    "accept_day",
)

BOOLEAN_COLUMNS = (
    "accept_gps_valid",
    "accept_is_weekend",
    "courier_prev_day_available",
    "city_prev_day_available",
)

HISTORICAL_NUMERIC_COLUMNS = tuple(
    column
    for column in HISTORICAL_FEATURE_COLUMNS
    if not column.endswith("_available")
)

NUMERIC_COLUMNS = BASE_NUMERIC_COLUMNS + HISTORICAL_NUMERIC_COLUMNS

MODEL_INPUT_COLUMNS = (
    NUMERIC_COLUMNS
    + CATEGORICAL_COLUMNS
    + BOOLEAN_COLUMNS
)


def _prepare_categorical_features(values: Any) -> Any:
    """Normalise les catégories et remplace les valeurs manquantes."""

    return (
        values.astype("string")
        .fillna("__missing__")
        .astype("object")
    )


def _cast_boolean_features_to_float(values: Any) -> Any:
    """Convertit les booléens en 0.0/1.0 pour scikit-learn."""

    return values.astype("float64")


def validate_preprocessing_frame(frame: pd.DataFrame) -> None:
    """Valide que toutes les colonnes nécessaires au modèle sont présentes."""

    required_columns = set(MODEL_INPUT_COLUMNS) | {TARGET_COLUMN}
    missing_columns = sorted(required_columns - set(frame.columns))

    if missing_columns:
        joined = ", ".join(missing_columns)
        raise ValueError(f"Missing ML columns: {joined}")


def prepare_target(target: pd.Series) -> pd.Series:
    """Convertit la cible binaire en entier 0/1."""

    if target.isna().any():
        raise ValueError("Target contains missing values")

    unique_values = set(target.unique().tolist())

    # False == 0 et True == 1 en Python.
    if not unique_values.issubset({0, 1}):
        raise ValueError(
            "Target must contain only boolean or binary 0/1 values"
        )

    return target.astype("int8")


def split_features_target(
    frame: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.Series]:
    """Sépare les features autorisées de la cible."""

    validate_preprocessing_frame(frame)

    features = frame.loc[:, MODEL_INPUT_COLUMNS].copy()
    target = prepare_target(frame[TARGET_COLUMN])

    return features, target


def build_preprocessor(
    *,
    scale_numeric: bool = True,
) -> ColumnTransformer:
    """Construit le preprocessing ML V1.

    Numériques :
        médiane + standardisation optionnelle.

    Catégorielles faible cardinalité :
        normalisation texte + valeur "__missing__" + one-hot encoding.

    Booléennes :
        conversion 0/1 + imputation à 0 si nécessaire.

    Les identifiants haute cardinalité courier_id et aoi_id sont exclus.
    """

    numeric_steps: list[tuple[str, Any]] = [
        (
            "imputer",
            SimpleImputer(
                strategy="median",
                keep_empty_features=True,
            ),
        ),
    ]

    if scale_numeric:
        numeric_steps.append(
            (
                "scaler",
                StandardScaler(),
            )
        )

    numeric_pipeline = Pipeline(numeric_steps)

    categorical_pipeline = Pipeline(
        [
            (
                "prepare",
                FunctionTransformer(
                    _prepare_categorical_features,
                    validate=False,
                    feature_names_out="one-to-one",
                ),
            ),
            (
                "encoder",
                OneHotEncoder(
                    handle_unknown="ignore",
                    sparse_output=True,
                ),
            ),
        ]
    )

    boolean_pipeline = Pipeline(
        [
            (
                "cast",
                FunctionTransformer(
                    _cast_boolean_features_to_float,
                    validate=False,
                    feature_names_out="one-to-one",
                ),
            ),
            (
                "imputer",
                SimpleImputer(
                    strategy="constant",
                    fill_value=0.0,
                    keep_empty_features=True,
                ),
            ),
        ]
    )

    return ColumnTransformer(
        transformers=[
            (
                "numeric",
                numeric_pipeline,
                list(NUMERIC_COLUMNS),
            ),
            (
                "categorical",
                categorical_pipeline,
                list(CATEGORICAL_COLUMNS),
            ),
            (
                "boolean",
                boolean_pipeline,
                list(BOOLEAN_COLUMNS),
            ),
        ],
        remainder="drop",
        sparse_threshold=1.0,
        verbose_feature_names_out=False,
    )