from __future__ import annotations

import hashlib
import json
import platform
from pathlib import Path

import lightgbm
import pandas as pd
import sklearn

from smartlogix.ml.artifact import (
    DEFAULT_METADATA_PATH,
    FINAL_MODEL_NAME,
    FINAL_MODEL_VERSION,
    FINAL_THRESHOLD,
    ModelArtifactMetadata,
    load_model_artifact,
    predict_delay_risk,
    save_model_artifact,
)
from smartlogix.ml.preprocessing import (
    MODEL_INPUT_COLUMNS,
    split_features_target,
)
from smartlogix.ml.training import (
    build_lightgbm_pipeline,
)

TRAIN_PATH = Path("data/ml_train.csv")
VALIDATION_PATH = Path("data/ml_validation.csv")


def load_dataset(path: Path) -> pd.DataFrame:
    """Charge un dataset ML exporté depuis PostgreSQL."""

    if not path.exists():
        raise FileNotFoundError(
            f"Dataset not found: {path}"
        )

    return pd.read_csv(
        path,
        true_values=["t"],
        false_values=["f"],
    )


def sha256_file(path: Path) -> str:
    """Calcule le SHA-256 d'un fichier."""

    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def main() -> None:
    train_frame = load_dataset(TRAIN_PATH)
    validation_frame = load_dataset(
        VALIDATION_PATH
    )

    training_frame = pd.concat(
        [
            train_frame,
            validation_frame,
        ],
        ignore_index=True,
    )

    x_train, y_train = split_features_target(
        training_frame
    )

    print(
        f"Dataset déploiement: "
        f"{len(training_frame):,} lignes"
    )

    print(
        f"Taux de retard: "
        f"{y_train.mean() * 100:.2f}%"
    )

    print(
        f"Features modèle: "
        f"{len(MODEL_INPUT_COLUMNS)}"
    )

    print(
        f"Seuil figé: "
        f"{FINAL_THRESHOLD:.6f}"
    )

    model = build_lightgbm_pipeline()

    print("\nEntraînement LightGBM déployable...")

    model.fit(
        x_train,
        y_train,
    )

    metadata = ModelArtifactMetadata(
        model_name=FINAL_MODEL_NAME,
        model_version=FINAL_MODEL_VERSION,
        threshold=FINAL_THRESHOLD,
        training_rows=len(training_frame),
        training_late_rate=float(
            y_train.mean()
        ),
        feature_count=len(
            MODEL_INPUT_COLUMNS
        ),
        training_splits=(
            "train",
            "validation",
        ),
        test_used_for_training=False,
        python_version=platform.python_version(),
        sklearn_version=sklearn.__version__,
        lightgbm_version=lightgbm.__version__,
    )

    artifact_path = save_model_artifact(
        model=model,
        metadata=metadata,
    )

    loaded = load_model_artifact(
        artifact_path
    )

    sample = training_frame.head(100)

    predictions = predict_delay_risk(
        loaded,
        sample,
    )

    if len(predictions) != len(sample):
        raise RuntimeError(
            "Artifact validation returned "
            "an unexpected number of predictions"
        )

    if predictions["delay_probability"].isna().any():
        raise RuntimeError(
            "Artifact produced missing probabilities"
        )

    if not predictions[
        "delay_probability"
    ].between(0.0, 1.0).all():
        raise RuntimeError(
            "Artifact produced invalid probabilities"
        )

    artifact_hash = sha256_file(
        artifact_path
    )

    manifest = {
        **metadata.as_dict(),
        "artifact_file": artifact_path.name,
        "artifact_sha256": artifact_hash,
        "artifact_size_bytes": (
            artifact_path.stat().st_size
        ),
    }

    DEFAULT_METADATA_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    DEFAULT_METADATA_PATH.write_text(
        json.dumps(
            manifest,
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    print("\n=== ARTEFACT ML CRÉÉ ===")
    print(
        json.dumps(
            manifest,
            indent=2,
            ensure_ascii=False,
        )
    )

    print(
        "\nTest chargement/inférence: OK"
    )


if __name__ == "__main__":
    main()