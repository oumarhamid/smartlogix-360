from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from smartlogix.streaming.accepted_events import (
    DeliveryAcceptedEvent,
)


def build_gold_record() -> dict:
    return {
        "order_id": 1,
        "region_id": 48,
        "city": "Chongqing",
        "courier_id": 100,
        "aoi_id": 200,
        "aoi_type": 14,
        "accept_timestamp": datetime(
            2000,
            10,
            15,
            8,
            30,
            tzinfo=UTC,
        ),
        "sla_minutes": 240.0,
        "accept_gps_lng": 107.1,
        "accept_gps_lat": 29.8,
        "accept_gps_valid": True,
    }


def test_delivery_accepted_event_can_be_created() -> None:
    event = DeliveryAcceptedEvent.from_gold_record(
        build_gold_record()
    )

    assert event.event_type == "delivery_accepted"
    assert event.order_id == 1
    assert event.region_id == 48
    assert event.city == "Chongqing"
    assert event.courier_id == 100
    assert event.aoi_id == 200
    assert event.aoi_type == 14
    assert event.sla_minutes == 240.0


def test_source_event_time_is_accept_timestamp() -> None:
    record = build_gold_record()

    event = DeliveryAcceptedEvent.from_gold_record(
        record
    )

    assert (
        event.source_event_time
        == record["accept_timestamp"]
    )


def test_event_contains_no_post_delivery_fields() -> None:
    record = build_gold_record()

    record.update(
        {
            "delivery_timestamp": datetime(
                2000,
                10,
                15,
                12,
                30,
                tzinfo=UTC,
            ),
            "delivery_duration_minutes": 240.0,
            "is_late_delivery": False,
            "is_within_sla": True,
            "delivery_gps_lng": 107.2,
            "delivery_gps_lat": 29.9,
        }
    )

    event = DeliveryAcceptedEvent.from_gold_record(
        record
    )

    payload = event.model_dump()

    assert "delivery_timestamp" not in payload
    assert "delivery_duration_minutes" not in payload
    assert "is_late_delivery" not in payload
    assert "is_within_sla" not in payload
    assert "delivery_gps_lng" not in payload
    assert "delivery_gps_lat" not in payload


def test_extra_post_delivery_field_is_rejected() -> None:
    event = DeliveryAcceptedEvent.from_gold_record(
        build_gold_record()
    )

    payload = event.model_dump()

    payload["delivery_timestamp"] = datetime(
        2000,
        10,
        15,
        12,
        30,
        tzinfo=UTC,
    )

    with pytest.raises(
        ValidationError
    ):
        DeliveryAcceptedEvent(
            **payload
        )


def test_courier_zero_is_allowed() -> None:
    record = build_gold_record()

    record["courier_id"] = 0

    event = DeliveryAcceptedEvent.from_gold_record(
        record
    )

    assert event.courier_id == 0


def test_naive_accept_timestamp_becomes_utc() -> None:
    record = build_gold_record()

    record["accept_timestamp"] = datetime(
        2000,
        10,
        15,
        8,
        30,
    )

    event = DeliveryAcceptedEvent.from_gold_record(
        record
    )

    assert (
        event.source_event_time.tzinfo
        is UTC
    )


def test_accept_gps_valid_can_be_reconstructed() -> None:
    record = build_gold_record()

    del record["accept_gps_valid"]

    event = DeliveryAcceptedEvent.from_gold_record(
        record
    )

    assert event.accept_gps_valid is True


def test_accept_timestamp_can_be_reconstructed_for_replay() -> None:
    record = build_gold_record()

    del record["accept_timestamp"]

    record["delivery_timestamp"] = datetime(
        2000,
        5,
        1,
        9,
        11,
        tzinfo=UTC,
    )

    record["delivery_duration_minutes"] = 180.0

    event = DeliveryAcceptedEvent.from_gold_record(
        record
    )

    assert event.source_event_time == datetime(
        2000,
        5,
        1,
        6,
        11,
        tzinfo=UTC,
    )


def test_negative_duration_reconstruction_is_rejected() -> None:
    record = build_gold_record()

    del record["accept_timestamp"]

    record["delivery_timestamp"] = datetime(
        2000,
        1,
        1,
        tzinfo=UTC,
    )

    record["delivery_duration_minutes"] = -100.0

    with pytest.raises(
        ValueError,
        match="negative",
    ):
        DeliveryAcceptedEvent.from_gold_record(
            record
        )


def test_missing_gps_is_reconstructed_as_invalid() -> None:
    record = build_gold_record()

    del record["accept_gps_valid"]

    record["accept_gps_lng"] = None
    record["accept_gps_lat"] = None

    event = DeliveryAcceptedEvent.from_gold_record(
        record
    )

    assert event.accept_gps_valid is False