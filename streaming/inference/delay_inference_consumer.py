from __future__ import annotations

import argparse
import json
import math
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import pandas as pd
from confluent_kafka import Consumer, KafkaError, Producer

from smartlogix.ml.artifact import (
    DEFAULT_ARTIFACT_PATH,
    LoadedModelArtifact,
    load_model_artifact,
    predict_delay_risk,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Consomme les features temps reel SmartLogix "
            "et produit les predictions LightGBM."
        )
    )

    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv(
            "KAFKA_INTERNAL_BOOTSTRAP_SERVERS",
            "kafka:19092",
        ),
    )

    parser.add_argument(
        "--input-topic",
        default=os.getenv(
            "KAFKA_DELIVERY_FEATURES_TOPIC",
            "smartlogix.delivery.features",
        ),
    )

    parser.add_argument(
        "--output-topic",
        default=os.getenv(
            "KAFKA_DELIVERY_PREDICTIONS_TOPIC",
            "smartlogix.delivery.predictions",
        ),
    )

    parser.add_argument(
        "--dead-letter-topic",
        default=os.getenv(
            "KAFKA_DEAD_LETTER_TOPIC",
            "smartlogix.delivery.dead-letter",
        ),
    )

    parser.add_argument(
        "--group-id",
        default=os.getenv(
            "ML_INFERENCE_CONSUMER_GROUP",
            "smartlogix-delay-inference-v1",
        ),
    )

    parser.add_argument(
        "--artifact-path",
        type=Path,
        default=DEFAULT_ARTIFACT_PATH,
    )

    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="0 = fonctionnement continu.",
    )

    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=10.0,
    )

    return parser.parse_args()


def utc_now_iso() -> str:
    return (
        datetime.now(UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def build_prediction_payload(
    payload: dict[str, Any],
    artifact: LoadedModelArtifact,
) -> dict[str, Any]:
    source_event_id = str(payload["event_id"])

    frame = pd.DataFrame([payload])

    result = predict_delay_risk(
        artifact,
        frame,
    ).iloc[0]

    probability = float(
        result["delay_probability"]
    )

    predicted_late = int(
        result["predicted_late"]
    )

    if not math.isfinite(probability):
        raise ValueError(
            "Model returned a non-finite delay probability"
        )

    if not 0.0 <= probability <= 1.0:
        raise ValueError(
            "Model returned a probability outside [0, 1]"
        )

    model_version = artifact.metadata.model_version

    prediction_id = str(
        uuid5(
            NAMESPACE_URL,
            (
                "smartlogix:"
                f"{model_version}:"
                f"{source_event_id}"
            ),
        )
    )

    return {
        "prediction_id": prediction_id,
        "event_type": "delivery_delay_prediction",
        "event_time": utc_now_iso(),
        "source_event_id": source_event_id,
        "source_event_time": payload.get(
            "source_event_time"
        ),
        "order_id": payload.get("order_id"),
        "region_id": payload.get("region_id"),
        "city": payload.get("city"),
        "courier_id": payload.get("courier_id"),
        "aoi_id": payload.get("aoi_id"),
        "delay_probability": probability,
        "predicted_late": predicted_late,
        "threshold": float(
            artifact.metadata.threshold
        ),
        "model_name": artifact.metadata.model_name,
        "model_version": model_version,
        "courier_prev_day_available": payload.get(
            "courier_prev_day_available"
        ),
        "city_prev_day_available": payload.get(
            "city_prev_day_available"
        ),
    }


def produce_sync(
    producer: Producer,
    *,
    topic: str,
    key: str,
    payload: dict[str, Any],
) -> None:
    delivery_errors: list[str] = []

    def callback(
        error: Exception | None,
        _message: Any,
    ) -> None:
        if error is not None:
            delivery_errors.append(str(error))

    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8"),
        on_delivery=callback,
    )

    remaining = producer.flush(10)

    if remaining:
        raise RuntimeError(
            f"{remaining} Kafka message(s) non livres"
        )

    if delivery_errors:
        raise RuntimeError(
            "Kafka delivery failed: "
            + "; ".join(delivery_errors)
        )


def build_dead_letter_payload(
    *,
    raw_payload: bytes | None,
    error: Exception,
    topic: str,
    partition: int,
    offset: int,
) -> dict[str, Any]:
    return {
        "event_type": "ml_inference_dead_letter",
        "event_time": utc_now_iso(),
        "source_topic": topic,
        "source_partition": partition,
        "source_offset": offset,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "raw_payload": (
            raw_payload.decode(
                "utf-8",
                errors="replace",
            )
            if raw_payload is not None
            else None
        ),
    }


def main() -> int:
    args = parse_args()

    if args.max_messages < 0:
        raise ValueError(
            "max-messages doit etre positif ou nul."
        )

    artifact = load_model_artifact(
        args.artifact_path
    )

    print(
        "MODEL_READY "
        f"name={artifact.metadata.model_name} "
        f"version={artifact.metadata.model_version} "
        f"threshold={artifact.metadata.threshold:.6f} "
        f"features={artifact.metadata.feature_count}"
    )

    consumer = Consumer(
        {
            "bootstrap.servers": (
                args.bootstrap_servers
            ),
            "group.id": args.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    producer = Producer(
        {
            "bootstrap.servers": (
                args.bootstrap_servers
            ),
            "client.id": (
                "smartlogix-delay-inference"
            ),
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
        }
    )

    consumer.subscribe(
        [args.input_topic]
    )

    processed = 0
    predicted_late_count = 0
    dead_letter_count = 0

    print(
        "INFERENCE_STARTED "
        f"input={args.input_topic} "
        f"output={args.output_topic} "
        f"group={args.group_id}"
    )

    try:
        while (
            args.max_messages == 0
            or processed < args.max_messages
        ):
            message = consumer.poll(
                args.timeout_seconds
            )

            if message is None:
                if args.max_messages:
                    print(
                        "Aucun nouvel evenement "
                        "avant expiration du delai."
                    )
                    break
                continue

            if message.error():
                if (
                    message.error().code()
                    == KafkaError._PARTITION_EOF
                ):
                    continue

                raise RuntimeError(
                    str(message.error())
                )

            raw_payload = message.value()

            try:
                payload = json.loads(
                    raw_payload.decode("utf-8")
                )

                prediction = (
                    build_prediction_payload(
                        payload,
                        artifact,
                    )
                )

                produce_sync(
                    producer,
                    topic=args.output_topic,
                    key=prediction[
                        "prediction_id"
                    ],
                    payload=prediction,
                )

                predicted_late_count += int(
                    prediction["predicted_late"]
                )

                print(
                    "PREDICTION "
                    f"order_id="
                    f"{prediction['order_id']} "
                    f"probability="
                    f"{prediction['delay_probability']:.4f} "
                    f"late="
                    f"{prediction['predicted_late']}"
                )

            except Exception as error:
                dead_letter = (
                    build_dead_letter_payload(
                        raw_payload=raw_payload,
                        error=error,
                        topic=message.topic(),
                        partition=message.partition(),
                        offset=message.offset(),
                    )
                )

                produce_sync(
                    producer,
                    topic=args.dead_letter_topic,
                    key=(
                        f"{message.topic()}:"
                        f"{message.partition()}:"
                        f"{message.offset()}"
                    ),
                    payload=dead_letter,
                )

                dead_letter_count += 1

                print(
                    "DEAD_LETTER "
                    f"partition="
                    f"{message.partition()} "
                    f"offset={message.offset()} "
                    f"error={error}"
                )

            consumer.commit(
                message=message,
                asynchronous=False,
            )

            processed += 1

    finally:
        producer.flush(10)
        consumer.close()

    print(
        "INFERENCE_FINISHED "
        f"processed={processed} "
        f"predicted_late="
        f"{predicted_late_count} "
        f"dead_letter={dead_letter_count}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())