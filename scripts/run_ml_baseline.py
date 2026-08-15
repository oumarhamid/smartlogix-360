from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd

from smartlogix.ml.metrics import evaluate_binary_predictions
from smartlogix.ml.preprocessing import split_features_target
from smartlogix.ml.training import (
    build_dummy_pipeline,
    build_logistic_pipeline,
    build_random_forest_pipeline,
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


def evaluate_model(
    *,
    name: str,
    model,
    x_train,
    y_train,
    x_validation,
    y_validation,
) -> dict[str, object]:
    """Entraîne puis évalue un modèle sur la validation temporelle."""

    started_at = time.perf_counter()

    model.fit(
        x_train,
        y_train,
    )

    training_seconds = time.perf_counter() - started_at

    predictions = model.predict(x_validation)
    probabilities = model.predict_proba(x_validation)[:, 1]

    metrics = evaluate_binary_predictions(
        y_validation,
        predictions,
        probabilities,
    )

    return {
        "model": name,
        "training_seconds": round(training_seconds, 3),
        **{
            key: round(value, 6)
            for key, value in metrics.as_dict().items()
        },
    }


def main() -> None:
    train_frame = load_dataset(TRAIN_PATH)
    validation_frame = load_dataset(VALIDATION_PATH)

    x_train, y_train = split_features_target(train_frame)

    x_validation, y_validation = split_features_target(
        validation_frame
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
            build_dummy_pipeline(),
        ),
        (
            "logistic",
            build_logistic_pipeline(),
        ),
        (
            "logistic_balanced",
            build_logistic_pipeline(
                class_weight="balanced",
            ),
        ),
        (
            "random_forest",
            build_random_forest_pipeline(),
        ),
    )

    results = []

    for name, model in experiments:
        print(f"\nEntraînement: {name}")

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

    print("\n=== Comparaison ===")

    results.sort(
        key=lambda item: item["pr_auc"],
        reverse=True,
    )

    for result in results:
        print(
            f"{result['model']:<20} "
            f"PR-AUC={result['pr_auc']:.4f} "
            f"ROC-AUC={result['roc_auc']:.4f} "
            f"Precision={result['precision']:.4f} "
            f"Recall={result['recall']:.4f} "
            f"F1={result['f1']:.4f} "
            f"BalancedAcc={result['balanced_accuracy']:.4f}"
        )


if __name__ == "__main__":
    main()