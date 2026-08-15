from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    date_sub,
    dayofmonth,
    dayofweek,
    from_json,
    hour,
    lit,
    month,
    pmod,
    struct,
    to_date,
    to_json,
    when,
)
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

ACCEPTED_EVENT_SCHEMA = StructType(
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
        StructField("sla_minutes", DoubleType(), False),
        StructField("accept_gps_lng", DoubleType(), True),
        StructField("accept_gps_lat", DoubleType(), True),
        StructField("accept_gps_valid", BooleanType(), False),
    ]
)


# Contrat exact attendu par le pipeline LightGBM V1.
MODEL_INPUT_COLUMNS = (
    "sla_minutes",
    "accept_gps_lng",
    "accept_gps_lat",
    "accept_hour",
    "accept_weekday",
    "accept_month",
    "accept_day",
    "courier_prev_day_orders_total",
    "courier_prev_day_orders_late",
    "courier_prev_day_avg_duration_minutes",
    "courier_prev_day_sla_compliance_rate",
    "courier_prev_day_quality_warning_rate",
    "city_prev_day_orders_total",
    "city_prev_day_orders_late",
    "city_prev_day_unique_couriers",
    "city_prev_day_avg_duration_minutes",
    "city_prev_day_sla_compliance_rate",
    "city_prev_day_quality_warning_rate",
    "region_id",
    "city",
    "aoi_type",
    "accept_period",
    "accept_gps_valid",
    "accept_is_weekend",
    "courier_prev_day_available",
    "city_prev_day_available",
)


METADATA_COLUMNS = (
    "event_id",
    "event_type",
    "event_time",
    "source_event_time",
    "order_id",
    "courier_id",
    "aoi_id",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Enrichit les delivery_accepted avec les features "
            "ML temps reel SmartLogix."
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
            "KAFKA_DELIVERY_ACCEPTED_TOPIC",
            "smartlogix.delivery.accepted",
        ),
    )

    parser.add_argument(
        "--output-topic",
        default=os.getenv(
            "KAFKA_DELIVERY_FEATURES_TOPIC",
            "smartlogix.delivery.features",
        ),
    )

    parser.add_argument(
        "--starting-offsets",
        default="earliest",
        choices=("earliest", "latest"),
    )

    parser.add_argument(
        "--checkpoint-location",
        default=(
            "/opt/smartlogix/data/checkpoints/"
            "streaming/delivery-accepted-features"
        ),
    )

    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Traite les evenements disponibles puis termine.",
    )

    return parser.parse_args()


def jdbc_url() -> str:
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.environ["POSTGRES_DATABASE"]

    return f"jdbc:postgresql://{host}:{port}/{database}"


def read_jdbc_table(
    spark: SparkSession,
    table_name: str,
) -> DataFrame:
    return (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url())
        .option("dbtable", table_name)
        .option("user", os.environ["POSTGRES_USER"])
        .option("password", os.environ["POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
        .load()
    )


def load_courier_history(
    spark: SparkSession,
) -> DataFrame:
    return (
        read_jdbc_table(
            spark,
            "analytics.courier_daily_performance",
        )
        .select(
            to_date(col("delivery_date")).alias(
                "_courier_delivery_date"
            ),
            col("region_id").alias("_courier_region_id"),
            col("city").alias("_courier_city"),
            col("courier_id").alias("_courier_match_id"),
            col("orders_total").alias(
                "courier_prev_day_orders_total"
            ),
            col("orders_late").alias(
                "courier_prev_day_orders_late"
            ),
            col("avg_duration_minutes").alias(
                "courier_prev_day_avg_duration_minutes"
            ),
            col("sla_compliance_rate").alias(
                "courier_prev_day_sla_compliance_rate"
            ),
            col("quality_warning_rate").alias(
                "courier_prev_day_quality_warning_rate"
            ),
        )
    )


def load_city_history(
    spark: SparkSession,
) -> DataFrame:
    return (
        read_jdbc_table(
            spark,
            "analytics.city_daily_performance",
        )
        .select(
            to_date(col("delivery_date")).alias(
                "_city_delivery_date"
            ),
            col("region_id").alias("_city_region_id"),
            col("city").alias("_city_match_name"),
            col("orders_total").alias(
                "city_prev_day_orders_total"
            ),
            col("orders_late").alias(
                "city_prev_day_orders_late"
            ),
            col("unique_couriers").alias(
                "city_prev_day_unique_couriers"
            ),
            col("avg_duration_minutes").alias(
                "city_prev_day_avg_duration_minutes"
            ),
            col("sla_compliance_rate").alias(
                "city_prev_day_sla_compliance_rate"
            ),
            col("quality_warning_rate").alias(
                "city_prev_day_quality_warning_rate"
            ),
        )
    )


def add_t0_features(
    events: DataFrame,
) -> DataFrame:
    result = events.withColumn(
        "accept_timestamp",
        col("source_event_time"),
    )

    result = result.withColumn(
        "_prev_delivery_date",
        date_sub(
            to_date(col("accept_timestamp")),
            1,
        ),
    )

    result = result.withColumn(
        "accept_hour",
        hour(col("accept_timestamp")).cast("int"),
    )

    # PostgreSQL ISODOW :
    # lundi=1 ... dimanche=7.
    result = result.withColumn(
        "accept_weekday",
        (
            pmod(
                dayofweek(col("accept_timestamp")) + lit(5),
                lit(7),
            )
            + lit(1)
        ).cast("int"),
    )

    result = result.withColumn(
        "accept_month",
        month(col("accept_timestamp")).cast("int"),
    )

    result = result.withColumn(
        "accept_day",
        dayofmonth(col("accept_timestamp")).cast("int"),
    )

    result = result.withColumn(
        "accept_is_weekend",
        col("accept_weekday").isin(6, 7),
    )

    result = result.withColumn(
        "accept_period",
        when(
            col("accept_hour") < 6,
            lit("night"),
        )
        .when(
            col("accept_hour") < 12,
            lit("morning"),
        )
        .when(
            col("accept_hour") < 18,
            lit("afternoon"),
        )
        .otherwise(lit("evening")),
    )

    return result


def enrich_with_history(
    events: DataFrame,
    courier_history: DataFrame,
    city_history: DataFrame,
) -> DataFrame:
    enriched = events.join(
        courier_history,
        (
            (
                events["region_id"]
                == courier_history["_courier_region_id"]
            )
            & (
                events["city"]
                == courier_history["_courier_city"]
            )
            & (
                events["courier_id"]
                == courier_history["_courier_match_id"]
            )
            & (
                events["_prev_delivery_date"]
                == courier_history["_courier_delivery_date"]
            )
        ),
        "left",
    )

    enriched = enriched.withColumn(
        "courier_prev_day_available",
        col("_courier_match_id").isNotNull(),
    )

    enriched = enriched.join(
        city_history,
        (
            (
                enriched["region_id"]
                == city_history["_city_region_id"]
            )
            & (
                enriched["city"]
                == city_history["_city_match_name"]
            )
            & (
                enriched["_prev_delivery_date"]
                == city_history["_city_delivery_date"]
            )
        ),
        "left",
    )

    enriched = enriched.withColumn(
        "city_prev_day_available",
        col("_city_match_name").isNotNull(),
    )

    return enriched


def build_kafka_output(
    enriched: DataFrame,
) -> DataFrame:
    missing = [
        name
        for name in MODEL_INPUT_COLUMNS
        if name not in enriched.columns
    ]

    if missing:
        raise RuntimeError(
            "Missing realtime ML features: "
            + ", ".join(missing)
        )

    if len(MODEL_INPUT_COLUMNS) != 26:
        raise RuntimeError(
            "Realtime ML contract must contain exactly 26 features"
        )

    payload_columns = (
        *METADATA_COLUMNS,
        *MODEL_INPUT_COLUMNS,
    )

    return enriched.select(
        col("event_id").cast("string").alias("key"),
        to_json(
            struct(
                *[
                    col(name)
                    for name in payload_columns
                ]
            ),
            options={
                "ignoreNullFields": "false",
            },
        ).alias("value"),
    )


def main() -> int:
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("smartlogix-accepted-feature-stream")
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    courier_history = load_courier_history(
        spark
    ).cache()

    city_history = load_city_history(
        spark
    ).cache()

    courier_rows = courier_history.count()
    city_rows = city_history.count()

    print(
        "STATIC_HISTORY_READY "
        f"courier_rows={courier_rows} "
        f"city_rows={city_rows}"
    )

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            args.bootstrap_servers,
        )
        .option(
            "subscribe",
            args.input_topic,
        )
        .option(
            "startingOffsets",
            args.starting_offsets,
        )
        .option(
            "failOnDataLoss",
            "false",
        )
        .load()
    )

    parsed = (
        kafka_stream
        .selectExpr(
            "CAST(value AS STRING) AS value"
        )
        .select(
            from_json(
                col("value"),
                ACCEPTED_EVENT_SCHEMA,
            ).alias("event")
        )
        .select("event.*")
        .where(
            col("event_id").isNotNull()
            & (
                col("event_type")
                == lit("delivery_accepted")
            )
            & col("source_event_time").isNotNull()
            & col("order_id").isNotNull()
        )
    )

    t0_features = add_t0_features(parsed)

    enriched = enrich_with_history(
        t0_features,
        courier_history,
        city_history,
    )

    kafka_output = build_kafka_output(
        enriched
    )

    writer = (
        kafka_output.writeStream
        .format("kafka")
        .outputMode("append")
        .option(
            "kafka.bootstrap.servers",
            args.bootstrap_servers,
        )
        .option(
            "topic",
            args.output_topic,
        )
        .option(
            "checkpointLocation",
            args.checkpoint_location,
        )
        .queryName(
            "smartlogix-delivery-accepted-features"
        )
    )

    if args.available_now:
        writer = writer.trigger(
            availableNow=True
        )
    else:
        writer = writer.trigger(
            processingTime="5 seconds"
        )

    query = writer.start()

    try:
        query.awaitTermination()
    finally:
        if query.isActive:
            query.stop()

        courier_history.unpersist()
        city_history.unpersist()
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())