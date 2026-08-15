from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession
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

from smartlogix.streaming.postgres_sink import (
    RealtimePostgresConfig,
    initialize_realtime_schema,
    write_realtime_microbatch,
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
        description="Persist SmartLogix delivery events from Kafka to PostgreSQL."
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
        default=os.getenv(
            "STREAMING_POSTGRES_CHECKPOINT",
            "/opt/smartlogix/data/checkpoints/streaming/delivery-events-postgres",
        ),
    )
    parser.add_argument(
        "--processing-time",
        default=os.getenv("STREAMING_PROCESSING_TIME", "5 seconds"),
    )
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Traite tous les evenements disponibles puis termine le job.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    postgres_config = RealtimePostgresConfig.from_env()
    initialize_realtime_schema(postgres_config)

    spark = (
        SparkSession.builder.appName("smartlogix-delivery-postgres-stream")
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
        kafka_stream.selectExpr(
            "CAST(value AS STRING) AS value",
            "partition AS kafka_partition",
            "offset AS kafka_offset",
            "timestamp AS kafka_timestamp",
        )
        .select(
            from_json(col("value"), EVENT_SCHEMA).alias("event"),
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
        )
        .select(
            "event.*",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
        )
        .where(col("event_id").isNotNull())
        .where(col("order_id").isNotNull())
        .where(col("event_time").isNotNull())
    )

    def write_batch(batch_dataframe: DataFrame, batch_id: int) -> None:
        write_realtime_microbatch(
            batch_dataframe,
            batch_id,
            postgres_config,
        )

    writer = (
        parsed.writeStream.foreachBatch(write_batch)
        .queryName(postgres_config.query_name)
        .option("checkpointLocation", args.checkpoint_location)
    )

    if args.available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=args.processing_time)

    query = writer.start()

    try:
        query.awaitTermination()
    finally:
        query.stop()
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
