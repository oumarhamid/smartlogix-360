from __future__ import annotations

import gc
import json
import time
from pathlib import Path

import pandas as pd

from smartlogix.ml.metrics import evaluate_binary_predictions
from smartlogix.ml.preprocessing import split_features_target
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

TRAIN_PATH = Path("data/ml_train.csv")
VALIDATION_PATH = Path("data/ml_validation.csv")


def load_dataset(path: Path) -> pd.DataFrame:
    """Charge un dataset ML exporté depuis PostgreSQL."""

    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")

    return pd.read_csv(
        path,
        true_values=["t"],
        false_values=["f"],
    )


def get_model_scores(model, features):
    """Retourne les scores continus nécessaires aux métriques AUC."""

    if hasattr(model, "predict_proba"):
        return (
            model.predict_proba(features)[:, 1],
            "predict_proba",
        )

    if hasattr(model, "decision_function"):
        return (
            model.decision_function(features),
            "decision_function",
        )

    raise TypeError(
        "Model must expose predict_proba or decision_function"
    )


def evaluate_model(
    *,
    name: str,
    model,
    x_train,
    y_train,
    x_validation,
    y_validation,
) -> dict[str, object]:
    """Entraîne et évalue un modèle sur la validation temporelle."""

    training_started = time.perf_counter()

    model.fit(
        x_train,
        y_train,
    )

    training_seconds = (
        time.perf_counter() - training_started
    )

    inference_started = time.perf_counter()

    predictions = model.predict(x_validation)

    scores, score_method = get_model_scores(
        model,
        x_validation,
    )

    inference_seconds = (
        time.perf_counter() - inference_started
    )

    metrics = evaluate_binary_predictions(
        y_validation,
        predictions,
        scores,
    )

    return {
        "model": name,
        "training_seconds": round(
            training_seconds,
            3,
        ),
        "inference_seconds": round(
            inference_seconds,
            3,
        ),
        "score_method": score_method,
        **{
            key: round(value, 6)
            for key, value in metrics.as_dict().items()
        },
    }


def main() -> None:
    train_frame = load_dataset(TRAIN_PATH)
    validation_frame = load_dataset(
        VALIDATION_PATH
    )

    x_train, y_train = split_features_target(
        train_frame
    )

    x_validation, y_validation = (
        split_features_target(
            validation_frame
        )
    )

    print(
        f"Train: {len(train_frame):,} lignes "
        f"({y_train.mean() * 100:.2f}% retard)"
    )

    print(
        f"Validation: {len(validation_frame):,} lignes "
        f"({y_validation.mean() * 100:.2f}% retard)"
    )

    experiments = (
        (
            "dummy_prior",
            build_dummy_pipeline,
        ),
        (
            "logistic",
            build_logistic_pipeline,
        ),
        (
            "logistic_balanced",
            lambda: build_logistic_pipeline(
                class_weight="balanced",
            ),
        ),
        (
            "decision_tree",
            build_decision_tree_pipeline,
        ),
        (
            "random_forest",
            build_random_forest_pipeline,
        ),
        (
            "hist_gradient_boosting",
            build_hist_gradient_boosting_pipeline,
        ),
        (
            "xgboost",
            build_xgboost_pipeline,
        ),
        (
            "lightgbm",
            build_lightgbm_pipeline,
        ),
        (
            "linear_svm",
            build_linear_svm_pipeline,
        ),
    )

    results: list[dict[str, object]] = []

    for name, builder in experiments:
        print(
            f"\n{'=' * 60}"
            f"\nEntraînement: {name}"
            f"\n{'=' * 60}"
        )

        model = builder()

        result = evaluate_model(
            name=name,
            model=model,
            x_train=x_train,
            y_train=y_train,
            x_validation=x_validation,
            y_validation=y_validation,
        )

        results.append(result)

        print(
            json.dumps(
                result,
                indent=2,
                ensure_ascii=False,
            )
        )

        del model
        gc.collect()

    print(
        "\n"
        "================ COMPARAISON FINALE ================"
    )

    results.sort(
        key=lambda item: float(item["pr_auc"]),
        reverse=True,
    )

    for rank, result in enumerate(
        results,
        start=1,
    ):
        print(
            f"{rank:>2}. "
            f"{result['model']:<25} "
            f"PR-AUC={result['pr_auc']:.4f} "
            f"ROC-AUC={result['roc_auc']:.4f} "
            f"Precision={result['precision']:.4f} "
            f"Recall={result['recall']:.4f} "
            f"F1={result['f1']:.4f} "
            f"BalAcc={result['balanced_accuracy']:.4f} "
            f"Train={result['training_seconds']:.2f}s "
            f"Infer={result['inference_seconds']:.2f}s"
        )


if __name__ == "__main__":
    main()