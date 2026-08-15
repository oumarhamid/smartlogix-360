from datetime import UTC, datetime

from smartlogix.streaming.events import DeliveryEvent


def test_delivery_event_from_gold_record() -> None:
    record = {
        "order_id": 10,
        "region_id": 2,
        "city": "Shanghai",
        "courier_id": 3,
        "aoi_id": 4,
        "aoi_type": 1,
        "delivery_timestamp": datetime(2026, 1, 1, 12, tzinfo=UTC),
        "delivery_duration_minutes": 250.5,
        "sla_minutes": 240.0,
        "is_within_sla": False,
        "is_late_delivery": True,
        "is_quality_warning": False,
        "accept_gps_lng": 121.4,
        "accept_gps_lat": 31.2,
        "delivery_gps_lng": 121.5,
        "delivery_gps_lat": 31.3,
    }

    event = DeliveryEvent.from_gold_record(
        record,
        event_time=datetime(2026, 8, 15, 0, tzinfo=UTC),
    )

    assert event.order_id == 10
    assert event.city == "Shanghai"
    assert event.is_late_delivery is True
    assert event.source_event_time == datetime(2026, 1, 1, 12, tzinfo=UTC)
    assert event.event_time == datetime(2026, 8, 15, 0, tzinfo=UTC)


def test_delivery_event_accepts_zero_courier_id() -> None:
    event = DeliveryEvent(
        event_id="evt-1",
        event_time=datetime.now(UTC),
        source_event_time=datetime.now(UTC),
        order_id=1,
        region_id=0,
        city="Shanghai",
        courier_id=0,
        aoi_id=1,
        aoi_type=0,
        delivery_duration_minutes=10.0,
        sla_minutes=240.0,
        is_within_sla=True,
        is_late_delivery=False,
        is_quality_warning=False,
    )

    assert event.courier_id == 0
