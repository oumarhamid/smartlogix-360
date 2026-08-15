from __future__ import annotations

from dataclasses import asdict, dataclass

from sklearn.metrics import (
    average_precision_score,
    balanced_accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)


@dataclass(frozen=True)
class BinaryClassificationMetrics:
    """Métriques principales pour la prédiction des retards."""

    roc_auc: float
    pr_auc: float
    precision: float
    recall: float
    f1: float
    balanced_accuracy: float
    predicted_positive_rate: float

    def as_dict(self) -> dict[str, float]:
        """Retourne les métriques sous forme de dictionnaire."""

        return asdict(self)


def evaluate_binary_predictions(
    y_true,
    y_pred,
    y_score,
) -> BinaryClassificationMetrics:
    """Évalue des prédictions binaires de retard."""

    if len(y_true) != len(y_pred) or len(y_true) != len(y_score):
        raise ValueError("Prediction arrays must have identical lengths")

    if len(y_true) == 0:
        raise ValueError("Cannot evaluate an empty dataset")

    return BinaryClassificationMetrics(
        roc_auc=float(roc_auc_score(y_true, y_score)),
        pr_auc=float(average_precision_score(y_true, y_score)),
        precision=float(
            precision_score(
                y_true,
                y_pred,
                zero_division=0,
            )
        ),
        recall=float(
            recall_score(
                y_true,
                y_pred,
                zero_division=0,
            )
        ),
        f1=float(
            f1_score(
                y_true,
                y_pred,
                zero_division=0,
            )
        ),
        balanced_accuracy=float(
            balanced_accuracy_score(
                y_true,
                y_pred,
            )
        ),
        predicted_positive_rate=float(sum(int(value) for value in y_pred) / len(y_pred)),
    )