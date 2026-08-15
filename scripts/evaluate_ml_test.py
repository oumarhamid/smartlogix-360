from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd
from sklearn.metrics import confusion_matrix

from smartlogix.ml.metrics import evaluate_binary_predictions
from smartlogix.ml.preprocessing import split_features_target
from smartlogix.ml.threshold import apply_threshold
from smartlogix.ml.training import build_lightgbm_pipeline

TRAIN_PATH = Path("data/ml_train.csv")
TEST_PATH = Path("data/ml_test.csv")

# Seuil sélectionné exclusivement sur le jeu de validation.
LIGHTGBM_THRESHOLD = 0.246906


def load_dataset(path: Path) -> pd.DataFrame:
    """Charge un dataset ML exporté depuis PostgreSQL."""

    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")

    return pd.read_csv(
        path,
        true_values=["t"],
        false_values=["f"],
    )


def main() -> None:
    train_frame = load_dataset(TRAIN_PATH)
    test_frame = load_dataset(TEST_PATH)

    x_train, y_train = split_features_target(train_frame)
    x_test, y_test = split_features_target(test_frame)

    print(
        f"Train: {len(train_frame):,} lignes "
        f"({y_train.mean() * 100:.2f}% retard)"
    )

    print(
        f"Test: {len(test_frame):,} lignes "
        f"({y_test.mean() * 100:.2f}% retard)"
    )

    print(
        f"Seuil LightGBM figé: {LIGHTGBM_THRESHOLD:.6f}"
    )

    model = build_lightgbm_pipeline()

    print("\nEntraînement du modèle final...")

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
        x_test
    )[:, 1]

    predictions = apply_threshold(
        scores,
        LIGHTGBM_THRESHOLD,
    )

    inference_seconds = (
        time.perf_counter() - inference_started
    )

    metrics = evaluate_binary_predictions(
        y_test,
        predictions,
        scores,
    )

    tn, fp, fn, tp = confusion_matrix(
        y_test,
        predictions,
        labels=[0, 1],
    ).ravel()

    global_result = {
        "model": "lightgbm",
        "threshold": LIGHTGBM_THRESHOLD,
        "train_rows": len(train_frame),
        "test_rows": len(test_frame),
        "test_late_rate": round(
            float(y_test.mean()),
            6,
        ),
        "training_seconds": round(
            training_seconds,
            3,
        ),
        "inference_seconds": round(
            inference_seconds,
            3,
        ),
        **{
            key: round(value, 6)
            for key, value in metrics.as_dict().items()
        },
        "confusion_matrix": {
            "true_negative": int(tn),
            "false_positive": int(fp),
            "false_negative": int(fn),
            "true_positive": int(tp),
        },
    }

    print("\n=== ÉVALUATION FINALE GLOBALE ===")

    print(
        json.dumps(
            global_result,
            indent=2,
            ensure_ascii=False,
        )
    )

    result_frame = test_frame[
        ["city", "is_late_delivery"]
    ].copy()

    result_frame["prediction"] = predictions
    result_frame["score"] = scores

    print("\n=== RÉSULTATS PAR VILLE ===")

    city_results = []

    for city, city_frame in result_frame.groupby(
        "city",
        sort=True,
    ):
        city_target = city_frame[
            "is_late_delivery"
        ].astype("int8")

        city_predictions = city_frame[
            "prediction"
        ]

        city_scores = city_frame[
            "score"
        ]

        result = {
            "city": city,
            "rows": len(city_frame),
            "late_rate": round(
                float(city_target.mean()),
                6,
            ),
        }

        if city_target.nunique() >= 2:
            city_metrics = evaluate_binary_predictions(
                city_target,
                city_predictions,
                city_scores,
            )

            result.update(
                {
                    key: round(value, 6)
                    for key, value
                    in city_metrics.as_dict().items()
                }
            )
        else:
            result["warning"] = (
                "Only one target class available"
            )

        city_results.append(result)

        print(
            json.dumps(
                result,
                indent=2,
                ensure_ascii=False,
            )
        )

    output = {
        "global": global_result,
        "by_city": city_results,
    }

    output_path = Path(
        "data/ml_lightgbm_test_results.json"
    )

    output_path.write_text(
        json.dumps(
            output,
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    print(
        f"\nRésultats enregistrés dans {output_path}"
    )


if __name__ == "__main__":
    main()