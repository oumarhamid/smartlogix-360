import pytest
from lightgbm import LGBMClassifier
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import (
    HistGradientBoostingClassifier,
    RandomForestClassifier,
)
from sklearn.linear_model import LogisticRegression
from sklearn.svm import LinearSVC
from sklearn.tree import DecisionTreeClassifier
from xgboost import XGBClassifier

from smartlogix.ml.training import (
    build_decision_tree_pipeline,
    build_dummy_pipeline,
    build_hist_gradient_boosting_pipeline,
    build_lightgbm_pipeline,
    build_linear_svm_pipeline,
    build_logistic_pipeline,
    build_random_forest_pipeline,
    build_xgboost_pipeline,
)


def test_build_dummy_pipeline() -> None:
    pipeline = build_dummy_pipeline()

    assert isinstance(
        pipeline.named_steps["model"],
        DummyClassifier,
    )


def test_build_logistic_pipeline() -> None:
    pipeline = build_logistic_pipeline()

    model = pipeline.named_steps["model"]

    assert isinstance(model, LogisticRegression)
    assert model.class_weight is None
    assert model.random_state == 42


def test_build_balanced_logistic_pipeline() -> None:
    pipeline = build_logistic_pipeline(
        class_weight="balanced",
    )

    assert (
        pipeline.named_steps["model"].class_weight
        == "balanced"
    )


def test_rejects_invalid_class_weight() -> None:
    with pytest.raises(
        ValueError,
        match="class_weight",
    ):
        build_logistic_pipeline(
            class_weight="invalid",
        )


def test_build_decision_tree_pipeline() -> None:
    pipeline = build_decision_tree_pipeline()

    model = pipeline.named_steps["model"]

    assert isinstance(
        model,
        DecisionTreeClassifier,
    )
    assert model.max_depth == 16
    assert model.class_weight == "balanced"


def test_build_random_forest_pipeline() -> None:
    pipeline = build_random_forest_pipeline()

    model = pipeline.named_steps["model"]

    assert isinstance(
        model,
        RandomForestClassifier,
    )
    assert model.n_estimators == 150
    assert model.max_depth == 18
    assert model.class_weight == "balanced_subsample"


def test_build_hist_gradient_boosting_pipeline() -> None:
    pipeline = build_hist_gradient_boosting_pipeline()

    assert "dense" in pipeline.named_steps

    model = pipeline.named_steps["model"]

    assert isinstance(
        model,
        HistGradientBoostingClassifier,
    )
    assert model.max_iter == 150
    assert model.class_weight == "balanced"


def test_build_linear_svm_pipeline() -> None:
    pipeline = build_linear_svm_pipeline()

    model = pipeline.named_steps["model"]

    assert isinstance(model, LinearSVC)
    assert model.class_weight == "balanced"
    assert model.max_iter == 5000


def test_build_xgboost_pipeline() -> None:
    pipeline = build_xgboost_pipeline()

    model = pipeline.named_steps["model"]

    assert isinstance(model, XGBClassifier)
    assert model.n_estimators == 250
    assert model.tree_method == "hist"
    assert model.random_state == 42


def test_build_lightgbm_pipeline() -> None:
    pipeline = build_lightgbm_pipeline()

    model = pipeline.named_steps["model"]

    assert isinstance(model, LGBMClassifier)
    assert model.n_estimators == 250
    assert model.random_state == 42