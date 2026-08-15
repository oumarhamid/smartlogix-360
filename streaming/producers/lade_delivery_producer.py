from __future__ import annotations

import argparse
import os
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Literal

from confluent_kafka import Producer

from smartlogix.streaming.accepted_events import DeliveryAcceptedEvent
from smartlogix.streaming.events import DeliveryEvent
from smartlogix.streaming.replay import (
    iter_parquet_records,
    round_robin_records,
)

CITY_CODES = ("jl", "yt", "cq", "sh", "hz")

DEFAULT_GOLD_ROOT = Path(
    "data/gold/lade/delivery/by_city"
)

EventMode = Literal[
    "delivery",
    "accepted",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Rejoue les livraisons Gold LaDe "
            "sous forme d'evenements Kafka."
        )
    )

    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092",
        ),
    )

    parser.add_argument(
        "--topic",
        default=None,
        help=(
            "Topic Kafka cible. "
            "Si absent, il est choisi selon --event-mode."
        ),
    )

    parser.add_argument(
        "--event-mode",
        choices=(
            "delivery",
            "accepted",
        ),
        default="delivery",
        help=(
            "delivery = evenement historique complet ; "
            "accepted = evenement T0 sans leakage."
        ),
    )

    parser.add_argument(
        "--gold-root",
        type=Path,
        default=DEFAULT_GOLD_ROOT,
    )

    parser.add_argument(
        "--cities",
        nargs="+",
        choices=CITY_CODES,
        default=list(CITY_CODES),
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=10_000,
    )

    parser.add_argument(
        "--events-per-second",
        type=float,
        default=10.0,
    )

    parser.add_argument(
        "--max-events",
        type=int,
        default=100,
        help=(
            "0 signifie rejouer tous "
            "les evenements disponibles."
        ),
    )

    return parser.parse_args()


def resolve_topic(
    event_mode: EventMode,
) -> str:
    """Retourne le topic par défaut correspondant au type d'événement."""

    if event_mode == "accepted":
        return os.getenv(
            "KAFKA_DELIVERY_ACCEPTED_TOPIC",
            "smartlogix.delivery.accepted",
        )

    return os.getenv(
        "KAFKA_DELIVERY_EVENTS_TOPIC",
        "smartlogix.delivery.events",
    )


def build_sources(
    gold_root: Path,
    cities: list[str],
    batch_size: int,
) -> dict[str, Iterator[dict[str, Any]]]:
    """Construit une source Gold par ville."""

    sources: dict[
        str,
        Iterator[dict[str, Any]],
    ] = {}

    for city_code in cities:
        parquet_path = (
            gold_root
            / city_code
            / "delivery_fact.parquet"
        )

        sources[city_code] = iter_parquet_records(
            parquet_path,
            batch_size=batch_size,
        )

    return sources


def build_event(
    record: dict[str, Any],
    event_mode: EventMode,
) -> DeliveryEvent | DeliveryAcceptedEvent:
    """Construit le contrat Kafka demandé."""

    if event_mode == "accepted":
        return DeliveryAcceptedEvent.from_gold_record(
            record
        )

    return DeliveryEvent.from_gold_record(
        record
    )


def delivery_callback(
    error: Exception | None,
    message: Any,
) -> None:
    """Signale les erreurs de livraison Kafka."""

    if error is not None:
        print(
            f"Echec de livraison Kafka: {error}"
        )


def main() -> int:
    args = parse_args()

    if args.events_per_second <= 0:
        raise ValueError(
            "events-per-second doit etre "
            "strictement positif."
        )

    if args.max_events < 0:
        raise ValueError(
            "max-events doit etre positif ou nul."
        )

    event_mode: EventMode = args.event_mode

    topic = (
        args.topic
        if args.topic
        else resolve_topic(event_mode)
    )

    producer = Producer(
        {
            "bootstrap.servers": (
                args.bootstrap_servers
            ),
            "client.id": (
                "smartlogix-lade-replay-producer"
            ),
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
        }
    )

    sources = build_sources(
        args.gold_root,
        args.cities,
        args.batch_size,
    )

    records = round_robin_records(
        sources
    )

    interval_seconds = (
        1.0 / args.events_per_second
    )

    sent = 0
    skipped = 0

    print(
        "SmartLogix producer demarre | "
        f"Kafka={args.bootstrap_servers} | "
        f"topic={topic} | "
        f"mode={event_mode} | "
        f"villes={','.join(args.cities)}"
    )

    try:
        for record in records:
            if (
                args.max_events
                and sent >= args.max_events
            ):
                break

            try:
                event = build_event(
                    record,
                    event_mode,
                )
            except ValueError as error:
                if event_mode != "accepted":
                    raise

                skipped += 1

                if skipped <= 5 or skipped % 100 == 0:
                    print(
                        "Evenement accepted ignore: "
                        f"{error}"
                    )

                continue

            payload = (
                event.model_dump_json()
                .encode("utf-8")
            )

            while True:
                try:
                    producer.produce(
                        topic=topic,
                        key=str(
                            event.order_id
                        ).encode("utf-8"),
                        value=payload,
                        on_delivery=delivery_callback,
                    )

                    break

                except BufferError:
                    producer.poll(0.25)

            producer.poll(0)

            sent += 1

            if (
                sent == 1
                or sent % 100 == 0
            ):
                print(
                    f"Evenements publies: {sent}"
                )

            time.sleep(
                interval_seconds
            )

    finally:
        remaining = producer.flush(
            15
        )

    if remaining:
        raise RuntimeError(
            f"{remaining} evenement(s) "
            "Kafka non livres."
        )

    print(
        f"Replay termine: "
        f"{sent} evenement(s) publie(s), "
        f"{skipped} ignore(s)."
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())