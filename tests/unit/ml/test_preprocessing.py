from __future__ import annotations

import pandas as pd
import pytest

from smartlogix.ml.dataset import TARGET_COLUMN
from smartlogix.ml.preprocessing import (
    HIGH_CARDINALITY_COLUMNS,
    MODEL_INPUT_COLUMNS,
    build_preprocessor,
    prepare_target,
    split_features_target,
    validate_preprocessing_frame,
)


def build_test_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4],
            "accept_timestamp": pd.to_datetime(
                [
                    "2000-09-01T08:00:00Z",
                    "2000-09-01T12:00:00Z",
                    "2000-10-01T15:00:00Z",
                    "2000-10-15T20:00:00Z",
                ]
            ),
            "region_id": [1, 1, 2, 2],
            "city": [
                "Chongqing",
                "Chongqing",
                "Shanghai",
                "Hangzhou",
            ],
            "courier_id": [101, 102, 103, 104],
            "aoi_id": [1001, 1002, 1003, 1004],
            "aoi_type": [1, 2, 1, 3],
            "sla_minutes": [240, 240, 180, 240],
            "accept_gps_lng": [
                107.1,
                107.2,
                121.4,
                None,
            ],
            "accept_gps_lat": [
                29.8,
                29.9,
                31.2,
                None,
            ],
            "accept_gps_valid": [
                True,
                True,
                True,
                False,
            ],
            "accept_hour": [8, 12, 15, 20],
            "accept_weekday": [5, 5, 7, 7],
            "accept_month": [9, 9, 10, 10],
            "accept_day": [1, 1, 1, 15],
            "accept_is_weekend": [
                False,
                False,
                True,
                True,
            ],
            "accept_period": [
                "morning",
                "afternoon",
                "afternoon",
                "evening",
            ],
            "courier_prev_day_orders_total": [
                12,
                20,
                None,
                15,
            ],
            "courier_prev_day_orders_late": [
                2,
                4,
                None,
                3,
            ],
            "courier_prev_day_avg_duration_minutes": [
                180.0,
                200.0,
                None,
                190.0,
            ],
            "courier_prev_day_sla_compliance_rate": [
                0.83,
                0.80,
                None,
                0.80,
            ],
            "courier_prev_day_quality_warning_rate": [
                0.01,
                0.02,
                None,
                0.03,
            ],
            "courier_prev_day_available": [
                True,
                True,
                False,
                True,
            ],
            "city_prev_day_orders_total": [
                1000,
                1000,
                2000,
                1500,
            ],
            "city_prev_day_orders_late": [
                150,
                150,
                160,
                250,
            ],
            "city_prev_day_unique_couriers": [
                120,
                120,
                200,
                180,
            ],
            "city_prev_day_avg_duration_minutes": [
                175.0,
                175.0,
                150.0,
                195.0,
            ],
            "city_prev_day_sla_compliance_rate": [
                0.85,
                0.85,
                0.92,
                0.82,
            ],
            "city_prev_day_quality_warning_rate": [
                0.01,
                0.01,
                0.02,
                0.03,
            ],
            "city_prev_day_available": [
                True,
                True,
                True,
                True,
            ],
            TARGET_COLUMN: [
                False,
                True,
                False,
                True,
            ],
        }
    )


def test_high_cardinality_columns_are_not_model_inputs() -> None:
    for column in HIGH_CARDINALITY_COLUMNS:
        assert column not in MODEL_INPUT_COLUMNS


def test_model_input_columns_are_unique() -> None:
    assert len(MODEL_INPUT_COLUMNS) == len(set(MODEL_INPUT_COLUMNS))


def test_validate_preprocessing_frame_accepts_valid_frame() -> None:
    validate_preprocessing_frame(build_test_frame())


def test_validate_preprocessing_frame_rejects_missing_column() -> None:
    frame = build_test_frame().drop(columns=["city"])

    with pytest.raises(ValueError, match="city"):
        validate_preprocessing_frame(frame)


def test_prepare_target_converts_boolean_to_binary() -> None:
    target = pd.Series([False, True, False, True])

    result = prepare_target(target)

    assert result.tolist() == [0, 1, 0, 1]
    assert str(result.dtype) == "int8"


def test_prepare_target_rejects_missing_value() -> None:
    target = pd.Series([True, False, None])

    with pytest.raises(ValueError, match="missing"):
        prepare_target(target)


def test_prepare_target_rejects_non_binary_value() -> None:
    target = pd.Series([0, 1, 2])

    with pytest.raises(ValueError, match="binary"):
        prepare_target(target)


def test_split_features_target_excludes_identifiers() -> None:
    features, target = split_features_target(build_test_frame())

    assert tuple(features.columns) == MODEL_INPUT_COLUMNS
    assert "order_id" not in features.columns
    assert "accept_timestamp" not in features.columns
    assert "courier_id" not in features.columns
    assert "aoi_id" not in features.columns

    assert target.tolist() == [0, 1, 0, 1]


def test_preprocessor_handles_missing_values() -> None:
    features, _ = split_features_target(build_test_frame())

    preprocessor = build_preprocessor()
    transformed = preprocessor.fit_transform(features)

    assert transformed.shape[0] == len(features)


def test_preprocessor_handles_unknown_categories() -> None:
    frame = build_test_frame()

    train_features, _ = split_features_target(frame.iloc[:3])

    test_frame = frame.iloc[[3]].copy()
    test_frame["city"] = "UnknownCity"
    test_frame["accept_period"] = "unknown_period"

    test_features, _ = split_features_target(test_frame)

    preprocessor = build_preprocessor()
    preprocessor.fit(train_features)

    transformed = preprocessor.transform(test_features)

    assert transformed.shape[0] == 1


def test_preprocessor_exposes_feature_names() -> None:
    features, _ = split_features_target(build_test_frame())

    preprocessor = build_preprocessor()
    preprocessor.fit(features)

    names = set(preprocessor.get_feature_names_out())

    assert "sla_minutes" in names
    assert "courier_prev_day_orders_total" in names
    assert "city_prev_day_sla_compliance_rate" in names
    assert "accept_gps_valid" in names

    assert not any("courier_id" in name for name in names)
    assert not any("aoi_id" in name for name in names)


def test_preprocessor_can_disable_numeric_scaling() -> None:
    features, _ = split_features_target(build_test_frame())

    preprocessor = build_preprocessor(scale_numeric=False)
    transformed = preprocessor.fit_transform(features)

    assert transformed.shape[0] == len(features)