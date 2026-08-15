from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from sklearn.metrics import precision_recall_curve


@dataclass(frozen=True)
class ThresholdSelection:
    """Résultat d'une optimisation de seuil sur la validation."""

    threshold: float
    precision: float
    recall: float
    f1: float


def apply_threshold(
    scores,
    threshold: float,
) -> np.ndarray:
    """Convertit des scores probabilistes en prédictions 0/1."""

    if not 0.0 <= threshold <= 1.0:
        raise ValueError("threshold must be between 0 and 1")

    return (
        np.asarray(scores, dtype=float) >= threshold
    ).astype("int8")


def select_best_f1_threshold(
    y_true,
    y_score,
) -> ThresholdSelection:
    """Sélectionne le seuil qui maximise le F1 sur la validation."""

    if len(y_true) != len(y_score):
        raise ValueError(
            "Target and score arrays must have identical lengths"
        )

    if len(y_true) == 0:
        raise ValueError(
            "Cannot select threshold on an empty dataset"
        )

    precision, recall, thresholds = precision_recall_curve(
        y_true,
        y_score,
    )

    if len(thresholds) == 0:
        raise ValueError(
            "No classification threshold available"
        )

    precision = precision[:-1]
    recall = recall[:-1]

    denominator = precision + recall

    f1_scores = np.divide(
        2.0 * precision * recall,
        denominator,
        out=np.zeros_like(denominator),
        where=denominator != 0,
    )

    best_index = int(np.argmax(f1_scores))

    return ThresholdSelection(
        threshold=float(thresholds[best_index]),
        precision=float(precision[best_index]),
        recall=float(recall[best_index]),
        f1=float(f1_scores[best_index]),
    )