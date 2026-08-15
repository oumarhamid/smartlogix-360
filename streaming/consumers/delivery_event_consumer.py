from __future__ import annotations

import argparse
import json
import os

from confluent_kafka import Consumer, KafkaError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consumer de validation des evenements SmartLogix."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--topic",
        default=os.getenv(
            "KAFKA_DELIVERY_EVENTS_TOPIC",
            "smartlogix.delivery.events",
        ),
    )
    parser.add_argument("--max-messages", type=int, default=20)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.max_messages <= 0:
        raise ValueError("max-messages doit etre strictement positif.")

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": "smartlogix-validation-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([args.topic])

    received = 0
    print(f"SmartLogix consumer demarre | Kafka={args.bootstrap_servers} | topic={args.topic}")

    try:
        while received < args.max_messages:
            message = consumer.poll(args.timeout_seconds)

            if message is None:
                print("Aucun nouvel evenement avant expiration du delai.")
                break

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(str(message.error()))

            payload = json.loads(message.value().decode("utf-8"))
            received += 1
            print(
                json.dumps(
                    {
                        "partition": message.partition(),
                        "offset": message.offset(),
                        "event": payload,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
    finally:
        consumer.close()

    print(f"Evenements lus: {received}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
