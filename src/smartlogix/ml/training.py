from __future__ import annotations

import numpy as np
from lightgbm import LGBMClassifier
from scipy import sparse
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import (
    HistGradientBoostingClassifier,
    RandomForestClassifier,
)
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer
from sklearn.svm import LinearSVC
from sklearn.tree import DecisionTreeClassifier
from xgboost import XGBClassifier

from smartlogix.ml.preprocessing import build_preprocessor


def _to_dense(values):
    """Convertit une matrice sparse en matrice dense si nécessaire."""

    if sparse.issparse(values):
        return values.toarray()

    return np.asarray(values)


def build_dummy_pipeline() -> Pipeline:
    """Construit la baseline naïve basée sur la classe majoritaire."""

    return Pipeline(
        [
            (
                "preprocessor",
                build_preprocessor(),
            ),
            (
                "model",
                DummyClassifier(
                    strategy="prior",
                ),
            ),
        ]
    )


def build_logistic_pipeline(
    *,
    class_weight: str | None = None,
) -> Pipeline:
    """Construit une régression logistique."""

    if class_weight not in {None, "balanced"}:
        raise ValueError(
            "class_weight must be None or 'balanced'"
        )

    return Pipeline(
        [
            (
                "preprocessor",
                build_preprocessor(),
            ),
            (
                "model",
                LogisticRegression(
                    solver="liblinear",
                    max_iter=500,
                    class_weight=class_weight,
                    random_state=42,
                ),
            ),
        ]
    )


def build_decision_tree_pipeline() -> Pipeline:
    """Construit un arbre de décision régularisé."""

    return Pipeline(
        [
            (
                "preprocessor",
                build_preprocessor(
                    scale_numeric=False,
                ),
            ),
            (
                "model",
                DecisionTreeClassifier(
                    max_depth=16,
                    min_samples_leaf=20,
                    class_weight="balanced",
                    random_state=42,
                ),
            ),
        ]
    )


def build_random_forest_pipeline() -> Pipeline:
    """Construit un Random Forest équilibré."""

    return Pipeline(
        [
            (
                "preprocessor",
                build_preprocessor(
                    scale_numeric=False,
                ),
            ),
            (
                "model",
                RandomForestClassifier(
                    n_estimators=150,
                    max_depth=18,
                    min_samples_leaf=5,
                    max_features="sqrt",
                    class_weight="balanced_subsample",
                    n_jobs=-1,
                    random_state=42,
                ),
            ),
        ]
    )


def build_hist_gradient_boosting_pipeline() -> Pipeline:
    """Construit un HistGradientBoosting adapté aux données tabulaires."""

    return Pipeline(
        [
            (
                "preprocessor",
                build_preprocessor(
                    scale_numeric=False,
                ),
            ),
            (
                "dense",
                FunctionTransformer(
                    _to_dense,
                    validate=False,
                    feature_names_out="one-to-one",
                ),
            ),
            (
                "model",
                HistGradientBoostingClassifier(
                    learning_rate=0.08,
                    max_iter=150,
                    max_leaf_nodes=31,
                    min_samples_leaf=20,
                    l2_regularization=1.0,
                    early_stopping=True,
                    validation_fraction=0.1,
                    n_iter_no_change=15,
                    class_weight="balanced",
                    random_state=42,
                ),
            ),
        ]
    )


def build_linear_svm_pipeline() -> Pipeline:
    """Construit un SVM linéaire équilibré."""

    return Pipeline(
        [
            (
                "preprocessor",
                build_preprocessor(),
            ),
            (
                "model",
                LinearSVC(
                    C=1.0,
                    class_weight="balanced",
                    dual="auto",
                    max_iter=5000,
                    random_state=42,
                ),
            ),
        ]
    )


def build_xgboost_pipeline() -> Pipeline:
    """Construit un XGBoost de classification binaire."""

    return Pipeline(
        [
            (
                "preprocessor",
                build_preprocessor(
                    scale_numeric=False,
                ),
            ),
            (
                "model",
                XGBClassifier(
                    objective="binary:logistic",
                    eval_metric="logloss",
                    tree_method="hist",
                    n_estimators=250,
                    learning_rate=0.05,
                    max_depth=6,
                    min_child_weight=5,
                    subsample=0.9,
                    colsample_bytree=0.9,
                    reg_lambda=1.0,
                    n_jobs=-1,
                    random_state=42,
                ),
            ),
        ]
    )


def build_lightgbm_pipeline() -> Pipeline:
    """Construit un LightGBM de classification binaire."""

    return Pipeline(
        [
            (
                "preprocessor",
                build_preprocessor(
                    scale_numeric=False,
                ),
            ),
            (
                "model",
                LGBMClassifier(
                    objective="binary",
                    n_estimators=250,
                    learning_rate=0.05,
                    num_leaves=31,
                    min_child_samples=20,
                    subsample=0.9,
                    subsample_freq=1,
                    colsample_bytree=0.9,
                    reg_lambda=1.0,
                    n_jobs=-1,
                    random_state=42,
                    verbosity=-1,
                ),
            ),
        ]
    )