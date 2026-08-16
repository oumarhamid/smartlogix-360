from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import joblib
import pandas as pd

from smartlogix.ml.constants import (
    FINAL_MODEL_NAME as FINAL_MODEL_NAME,
)
from smartlogix.ml.constants import (
    FINAL_MODEL_VERSION as FINAL_MODEL_VERSION,
)
from smartlogix.ml.constants import (
    FINAL_THRESHOLD as FINAL_THRESHOLD,
)
from smartlogix.ml.preprocessing import MODEL_INPUT_COLUMNS
from smartlogix.ml.threshold import apply_threshold

DEFAULT_ARTIFACT_PATH = Path(
    "artifacts/ml/lightgbm_delay_v1.joblib"
)

DEFAULT_METADATA_PATH = Path(
    "artifacts/ml/lightgbm_delay_v1.metadata.json"
)


@dataclass(frozen=True)
class ModelArtifactMetadata:
    """Métadonnées nécessaires à la reproductibilité du modèle."""

    model_name: str
    model_version: str
    threshold: float
    training_rows: int
    training_late_rate: float
    feature_count: int
    training_splits: tuple[str, ...]
    test_used_for_training: bool
    python_version: str
    sklearn_version: str
    lightgbm_version: str

    def __post_init__(self) -> None:
        if not 0.0 <= self.threshold <= 1.0:
            raise ValueError(
                "threshold must be between 0 and 1"
            )

        if self.training_rows <= 0:
            raise ValueError(
                "training_rows must be strictly positive"
            )

        if self.feature_count <= 0:
            raise ValueError(
                "feature_count must be strictly positive"
            )

    def as_dict(self) -> dict[str, Any]:
        """Retourne les métadonnées sous forme de dictionnaire."""

        return asdict(self)


@dataclass(frozen=True)
class LoadedModelArtifact:
    """Modèle chargé avec ses métadonnées."""

    model: Any
    metadata: ModelArtifactMetadata


def prepare_inference_features(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    """Prépare les features nécessaires à une inférence sans target."""

    missing_columns = sorted(
        set(MODEL_INPUT_COLUMNS) - set(frame.columns)
    )

    if missing_columns:
        joined = ", ".join(missing_columns)
        raise ValueError(
            f"Missing inference columns: {joined}"
        )

    return frame.loc[:, MODEL_INPUT_COLUMNS].copy()


def save_model_artifact(
    *,
    model: Any,
    metadata: ModelArtifactMetadata,
    path: Path = DEFAULT_ARTIFACT_PATH,
) -> Path:
    """Sauvegarde le pipeline ML complet avec ses métadonnées."""

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    payload = {
        "model": model,
        "metadata": metadata.as_dict(),
    }

    joblib.dump(
        payload,
        path,
        compress=3,
    )

    return path


def load_model_artifact(
    path: Path = DEFAULT_ARTIFACT_PATH,
) -> LoadedModelArtifact:
    """Charge et valide un artefact ML SmartLogix."""

    if not path.exists():
        raise FileNotFoundError(
            f"Model artifact not found: {path}"
        )

    payload = joblib.load(path)

    if not isinstance(payload, dict):
        raise ValueError("Invalid model artifact payload")

    if "model" not in payload or "metadata" not in payload:
        raise ValueError(
            "Model artifact must contain model and metadata"
        )

    metadata_payload = dict(payload["metadata"])

    metadata_payload["training_splits"] = tuple(
        metadata_payload["training_splits"]
    )

    metadata = ModelArtifactMetadata(
        **metadata_payload
    )

    return LoadedModelArtifact(
        model=payload["model"],
        metadata=metadata,
    )


def predict_delay_risk(
    artifact: LoadedModelArtifact,
    frame: pd.DataFrame,
) -> pd.DataFrame:
    """Prédit la probabilité et la classe de retard."""

    features = prepare_inference_features(frame)

    probabilities = artifact.model.predict_proba(
        features
    )[:, 1]

    predictions = apply_threshold(
        probabilities,
        artifact.metadata.threshold,
    )

    return pd.DataFrame(
        {
            "delay_probability": probabilities,
            "predicted_late": predictions,
        },
        index=frame.index,
    )