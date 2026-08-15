from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("source_event_time", TimestampType(), False),
        StructField("order_id", LongType(), False),
        StructField("region_id", LongType(), False),
        StructField("city", StringType(), False),
        StructField("courier_id", LongType(), False),
        StructField("aoi_id", LongType(), False),
        StructField("aoi_type", IntegerType(), False),
        StructField("delivery_duration_minutes", DoubleType(), True),
        StructField("sla_minutes", DoubleType(), False),
        StructField("is_within_sla", BooleanType(), False),
        StructField("is_late_delivery", BooleanType(), False),
        StructField("is_quality_warning", BooleanType(), False),
        StructField("accept_gps_lng", DoubleType(), True),
        StructField("accept_gps_lat", DoubleType(), True),
        StructField("delivery_gps_lng", DoubleType(), True),
        StructField("delivery_gps_lat", DoubleType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Premier job Spark Structured Streaming de SmartLogix."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_INTERNAL_BOOTSTRAP_SERVERS", "kafka:19092"),
    )
    parser.add_argument(
        "--topic",
        default=os.getenv(
            "KAFKA_DELIVERY_EVENTS_TOPIC",
            "smartlogix.delivery.events",
        ),
    )
    parser.add_argument(
        "--checkpoint-location",
        default="/opt/smartlogix/data/checkpoints/streaming/delivery-events-console",
    )
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Traite les evenements disponibles puis termine le job.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("smartlogix-delivery-stream")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        kafka_stream.selectExpr("CAST(value AS STRING) AS value")
        .select(from_json(col("value"), EVENT_SCHEMA).alias("event"))
        .select("event.*")
        .where(col("event_id").isNotNull())
    )

    writer = (
        parsed.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", "20")
        .option("checkpointLocation", args.checkpoint_location)
    )

    if args.available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime="5 seconds")

    query = writer.start()

    try:
        query.awaitTermination()
    finally:
        query.stop()
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
