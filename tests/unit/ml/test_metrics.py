import pytest

from smartlogix.ml.metrics import evaluate_binary_predictions


def test_evaluate_perfect_predictions() -> None:
    metrics = evaluate_binary_predictions(
        [0, 0, 1, 1],
        [0, 0, 1, 1],
        [0.1, 0.2, 0.8, 0.9],
    )

    assert metrics.roc_auc == pytest.approx(1.0)
    assert metrics.pr_auc == pytest.approx(1.0)
    assert metrics.precision == pytest.approx(1.0)
    assert metrics.recall == pytest.approx(1.0)
    assert metrics.f1 == pytest.approx(1.0)
    assert metrics.balanced_accuracy == pytest.approx(1.0)
    assert metrics.predicted_positive_rate == pytest.approx(0.5)


def test_evaluate_rejects_different_lengths() -> None:
    with pytest.raises(ValueError, match="identical lengths"):
        evaluate_binary_predictions(
            [0, 1],
            [0],
            [0.1, 0.9],
        )


def test_evaluate_rejects_empty_dataset() -> None:
    with pytest.raises(ValueError, match="empty"):
        evaluate_binary_predictions(
            [],
            [],
            [],
        )