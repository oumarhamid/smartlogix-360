from __future__ import annotations

import gc
import json
import time
from pathlib import Path

import pandas as pd

from smartlogix.ml.metrics import evaluate_binary_predictions
from smartlogix.ml.preprocessing import split_features_target
from smartlogix.ml.threshold import (
    apply_threshold,
    select_best_f1_threshold,
)
from smartlogix.ml.training import (
    build_hist_gradient_boosting_pipeline,
    build_lightgbm_pipeline,
    build_random_forest_pipeline,
    build_xgboost_pipeline,
)

TRAIN_PATH = Path("data/ml_train.csv")
VALIDATION_PATH = Path("data/ml_validation.csv")


def load_dataset(path: Path) -> pd.DataFrame:
    """Charge un dataset ML local."""

    if not path.exists():
        raise FileNotFoundError(
            f"Dataset not found: {path}"
        )

    return pd.read_csv(
        path,
        true_values=["t"],
        false_values=["f"],
    )


def evaluate_candidate(
    *,
    name: str,
    builder,
    x_train,
    y_train,
    x_validation,
    y_validation,
) -> dict[str, object]:
    """Entraîne puis optimise le seuil d'un candidat."""

    model = builder()

    training_started = time.perf_counter()

    model.fit(
        x_train,
        y_train,
    )

    training_seconds = (
        time.perf_counter() - training_started
    )

    inference_started = time.perf_counter()

    scores = model.predict_proba(
        x_validation
    )[:, 1]

    inference_seconds = (
        time.perf_counter() - inference_started
    )

    default_predictions = apply_threshold(
        scores,
        0.5,
    )

    default_metrics = evaluate_binary_predictions(
        y_validation,
        default_predictions,
        scores,
    )

    selection = select_best_f1_threshold(
        y_validation,
        scores,
    )

    tuned_predictions = apply_threshold(
        scores,
        selection.threshold,
    )

    tuned_metrics = evaluate_binary_predictions(
        y_validation,
        tuned_predictions,
        scores,
    )

    result = {
        "model": name,
        "training_seconds": round(
            training_seconds,
            3,
        ),
        "inference_seconds": round(
            inference_seconds,
            3,
        ),
        "default": {
            "threshold": 0.5,
            **{
                key: round(value, 6)
                for key, value
                in default_metrics.as_dict().items()
            },
        },
        "optimized": {
            "threshold": round(
                selection.threshold,
                6,
            ),
            **{
                key: round(value, 6)
                for key, value
                in tuned_metrics.as_dict().items()
            },
        },
    }

    del model
    gc.collect()

    return result


def main() -> None:
    train_frame = load_dataset(
        TRAIN_PATH
    )
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

    candidates = (
        (
            "lightgbm",
            build_lightgbm_pipeline,
        ),
        (
            "xgboost",
            build_xgboost_pipeline,
        ),
        (
            "hist_gradient_boosting",
            build_hist_gradient_boosting_pipeline,
        ),
        (
            "random_forest",
            build_random_forest_pipeline,
        ),
    )

    results: list[dict[str, object]] = []

    for name, builder in candidates:
        print(
            f"\n{'=' * 60}"
            f"\nOptimisation: {name}"
            f"\n{'=' * 60}"
        )

        result = evaluate_candidate(
            name=name,
            builder=builder,
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

    results.sort(
        key=lambda item: float(
            item["optimized"]["f1"]
        ),
        reverse=True,
    )

    print(
        "\n"
        "========== CLASSEMENT APRÈS OPTIMISATION =========="
    )

    for rank, result in enumerate(
        results,
        start=1,
    ):
        optimized = result["optimized"]

        print(
            f"{rank}. "
            f"{result['model']:<25} "
            f"Threshold={optimized['threshold']:.4f} "
            f"PR-AUC={optimized['pr_auc']:.4f} "
            f"ROC-AUC={optimized['roc_auc']:.4f} "
            f"Precision={optimized['precision']:.4f} "
            f"Recall={optimized['recall']:.4f} "
            f"F1={optimized['f1']:.4f} "
            f"BalAcc={optimized['balanced_accuracy']:.4f}"
        )


if __name__ == "__main__":
    main()