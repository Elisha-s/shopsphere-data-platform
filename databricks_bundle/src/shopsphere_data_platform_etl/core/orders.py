"""Bronze and Silver transformations for order events."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from shopsphere_data_platform_etl.core.schemas import ORDER_EVENT_SCHEMA

# Where a message came from. Kept on every Bronze row so any record can be
# traced back to the exact message that produced it.
TRANSPORT_COLUMNS = [
    "message_key",
    "raw_json",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "enqueued_timestamp",
]

DEDUP_WINDOW = "1 day"


def kafka_to_transport(kafka_df: DataFrame) -> DataFrame:
    """Rename the Kafka source's columns to the Bronze transport columns.

    Works for a real Kafka broker and for Azure Event Hubs' Kafka endpoint.
    Key and value arrive as bytes, so they are cast to strings.
    """
    return kafka_df.select(
        F.col("key").cast("string").alias("message_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("enqueued_timestamp"),
    )


def parse_events(transport_df: DataFrame) -> DataFrame:
    """Build Bronze rows: transport metadata + raw JSON + the parsed event.

    Nothing is filtered here. A message that is not valid JSON still becomes
    a Bronze row (with NULL parsed fields), because Bronze is the replayable
    record of everything that arrived.
    """
    return (
        transport_df.withColumn("parsed_event", F.from_json(F.col("raw_json"), ORDER_EVENT_SCHEMA))
        .select(*TRANSPORT_COLUMNS, "parsed_event.*")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


def dedup_key() -> Column:
    """What makes two Bronze rows "the same event".

    Normally the producer's ``event_id``. A message too broken to have one
    falls back to a hash of its raw text, so two different broken messages
    are not mistaken for duplicates of each other.
    """
    return F.coalesce(F.col("event_id"), F.sha2(F.coalesce(F.col("raw_json"), F.lit("")), 256))


def flatten_orders(bronze_df: DataFrame) -> DataFrame:
    """Turn nested Bronze events into flat, typed order rows.

    ``try_to_timestamp`` returns NULL for an unparseable timestamp instead of
    failing the whole pipeline; the NULL is then caught by a quality rule.
    """
    return bronze_df.select(
        dedup_key().alias("dedup_key"),
        "event_id",
        "event_type",
        "event_version",
        F.try_to_timestamp(F.col("event_timestamp")).alias("event_timestamp"),
        F.try_to_timestamp(F.col("generated_at")).alias("generated_at"),
        "is_late_event",
        F.col("payload.order_id").alias("order_id"),
        F.col("payload.customer_id").alias("customer_id"),
        F.col("payload.product_id").alias("product_id"),
        F.col("payload.product_name").alias("product_name"),
        F.col("payload.category").alias("category"),
        F.col("payload.brand").alias("brand"),
        F.col("payload.quantity").alias("quantity"),
        F.col("payload.unit_price").alias("unit_price"),
        F.col("payload.total_amount").alias("total_amount"),
        F.col("payload.payment_method").alias("payment_method"),
        F.col("payload.order_status").alias("order_status"),
        F.col("payload.city").alias("city"),
        F.col("payload.state").alias("state"),
        F.col("payload.country").alias("country"),
        F.try_to_timestamp(F.col("payload.order_timestamp")).alias("order_timestamp"),
        "raw_json",
        "message_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "enqueued_timestamp",
        "ingestion_timestamp",
    )


def deduplicate_events(orders_stream: DataFrame, window: str = DEDUP_WINDOW) -> DataFrame:
    """Drop repeated deliveries of the same event (streaming only).

    Two design points:

    * ``dropDuplicatesWithinWatermark`` forgets a key once the watermark has
      moved ``window`` past it, so the state Spark keeps stays bounded. Plain
      ``dropDuplicates(["event_id"])`` would remember every event id forever.
    * The watermark is on the *arrival* time (``enqueued_timestamp``), not the
      business event time. Duplicates are a delivery problem, so they are
      close together in arrival time. Using arrival time also means a
      late-arriving event is never discarded here for being "too old".
    """
    return orders_stream.withWatermark("enqueued_timestamp", window).dropDuplicatesWithinWatermark(["dedup_key"])
