from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from smartlogix.ml.artifact import (
    FINAL_THRESHOLD,
    LoadedModelArtifact,
    ModelArtifactMetadata,
    load_model_artifact,
    predict_delay_risk,
    prepare_inference_features,
    save_model_artifact,
)
from smartlogix.ml.preprocessing import (
    MODEL_INPUT_COLUMNS,
)


class ConstantProbabilityModel:
    """Petit modèle déterministe utilisé pour les tests."""

    def __init__(
        self,
        probability: float,
    ) -> None:
        self.probability = probability

    def predict_proba(
        self,
        features,
    ) -> np.ndarray:
        rows = len(features)

        positive = np.full(
            rows,
            self.probability,
            dtype=float,
        )

        negative = 1.0 - positive

        return np.column_stack(
            [
                negative,
                positive,
            ]
        )


def build_metadata() -> ModelArtifactMetadata:
    return ModelArtifactMetadata(
        model_name="test-model",
        model_version="v1",
        threshold=0.25,
        training_rows=100,
        training_late_rate=0.2,
        feature_count=len(
            MODEL_INPUT_COLUMNS
        ),
        training_splits=(
            "train",
            "validation",
        ),
        test_used_for_training=False,
        python_version="3.11",
        sklearn_version="test",
        lightgbm_version="test",
    )


def build_inference_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            column: [0, 1]
            for column in MODEL_INPUT_COLUMNS
        }
    )


def test_final_threshold_is_valid() -> None:
    assert 0.0 <= FINAL_THRESHOLD <= 1.0


def test_prepare_inference_features() -> None:
    frame = build_inference_frame()

    result = prepare_inference_features(
        frame
    )

    assert tuple(result.columns) == (
        MODEL_INPUT_COLUMNS
    )


def test_prepare_inference_features_rejects_missing_column() -> None:
    frame = build_inference_frame().drop(
        columns=["city"]
    )

    with pytest.raises(
        ValueError,
        match="city",
    ):
        prepare_inference_features(
            frame
        )


def test_artifact_metadata_rejects_invalid_threshold() -> None:
    with pytest.raises(
        ValueError,
        match="threshold",
    ):
        ModelArtifactMetadata(
            model_name="test",
            model_version="v1",
            threshold=1.5,
            training_rows=10,
            training_late_rate=0.2,
            feature_count=1,
            training_splits=("train",),
            test_used_for_training=False,
            python_version="3.11",
            sklearn_version="test",
            lightgbm_version="test",
        )


def test_save_and_load_model_artifact(
    tmp_path: Path,
) -> None:
    path = tmp_path / "model.joblib"

    model = ConstantProbabilityModel(
        probability=0.7
    )

    metadata = build_metadata()

    save_model_artifact(
        model=model,
        metadata=metadata,
        path=path,
    )

    loaded = load_model_artifact(
        path
    )

    assert loaded.metadata == metadata
    assert (
        loaded.model.probability
        == pytest.approx(0.7)
    )


def test_predict_delay_risk() -> None:
    artifact = LoadedModelArtifact(
        model=ConstantProbabilityModel(
            probability=0.7
        ),
        metadata=build_metadata(),
    )

    result = predict_delay_risk(
        artifact,
        build_inference_frame(),
    )

    assert result[
        "delay_probability"
    ].tolist() == pytest.approx(
        [0.7, 0.7]
    )

    assert result[
        "predicted_late"
    ].tolist() == [1, 1]