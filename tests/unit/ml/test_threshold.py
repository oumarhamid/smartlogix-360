import numpy as np
import pytest

from smartlogix.ml.threshold import (
    apply_threshold,
    select_best_f1_threshold,
)


def test_apply_threshold() -> None:
    result = apply_threshold(
        [0.1, 0.49, 0.5, 0.9],
        0.5,
    )

    assert result.tolist() == [0, 0, 1, 1]
    assert result.dtype == np.int8


@pytest.mark.parametrize(
    "threshold",
    [-0.1, 1.1],
)
def test_apply_threshold_rejects_invalid_threshold(
    threshold: float,
) -> None:
    with pytest.raises(
        ValueError,
        match="between",
    ):
        apply_threshold(
            [0.2, 0.8],
            threshold,
        )


def test_best_threshold_returns_valid_values() -> None:
    selection = select_best_f1_threshold(
        [0, 0, 1, 1],
        [0.1, 0.4, 0.6, 0.9],
    )

    assert 0.0 <= selection.threshold <= 1.0
    assert 0.0 <= selection.precision <= 1.0
    assert 0.0 <= selection.recall <= 1.0
    assert 0.0 <= selection.f1 <= 1.0


def test_best_threshold_can_reach_perfect_f1() -> None:
    selection = select_best_f1_threshold(
        [0, 0, 1, 1],
        [0.1, 0.2, 0.8, 0.9],
    )

    assert selection.precision == pytest.approx(1.0)
    assert selection.recall == pytest.approx(1.0)
    assert selection.f1 == pytest.approx(1.0)


def test_best_threshold_rejects_different_lengths() -> None:
    with pytest.raises(
        ValueError,
        match="identical lengths",
    ):
        select_best_f1_threshold(
            [0, 1],
            [0.3],
        )


def test_best_threshold_rejects_empty_dataset() -> None:
    with pytest.raises(
        ValueError,
        match="empty",
    ):
        select_best_f1_threshold(
            [],
            [],
        )