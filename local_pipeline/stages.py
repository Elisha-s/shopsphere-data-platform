"""The pipeline stages, one function each.

Every stage processes whatever is new and then stops ("available now"
triggers), like a triggered Lakeflow pipeline update. Streaming checkpoints
remember what has been processed, so running a stage twice does nothing the
second time.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from local_pipeline.config import LocalConfig
from shopsphere_data_platform_etl.core import metrics
from shopsphere_data_platform_etl.core.orders import (
    deduplicate_events,
    flatten_orders,
    kafka_to_transport,
    parse_events,
)
from shopsphere_data_platform_etl.core.quality import invalid_orders, valid_orders
from shopsphere_data_platform_etl.core.tables import Tables
from shopsphere_data_platform_etl.jobs import cdf_merge, validate_outputs


def ensure_schemas(spark: SparkSession, tables: Tables) -> None:
    for schema in tables.schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def _write_stream_to_table(stream: DataFrame, table: str, checkpoint: str) -> None:
    """Append everything currently available to ``table``, then stop."""
    query = (
        stream.writeStream.outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(table)
    )
    query.awaitTermination()


def _replace_table_contents(spark: SparkSession, df: DataFrame, table: str) -> None:
    """Create ``table`` from ``df``, or replace all of its rows if it exists.

    ``df.write.mode("overwrite").saveAsTable(...)`` is not used: with Spark
    3.5.6 and Delta 3.2.0 it takes a "replace table" path that Delta rejects
    ("does not support truncate in batch mode"). Creating the table once and
    then overwriting its rows (INSERT OVERWRITE) avoids that path, and it
    also keeps the table's history instead of recreating the table each run.
    """
    if spark.catalog.tableExists(table):
        df.write.mode("overwrite").insertInto(table)
    else:
        df.write.saveAsTable(table)


# ----------------------------------------------------------------------
# Sources
# ----------------------------------------------------------------------
def read_landing_files(spark: SparkSession, config: LocalConfig) -> DataFrame:
    """Read newline-delimited JSON files as a stream, one row per line.

    The lines are read as plain text, not parsed as JSON, so a malformed line
    still reaches Bronze instead of being dropped by the reader.
    """
    os.makedirs(config.landing_dir, exist_ok=True)
    return (
        spark.readStream.format("text")
        .load(config.landing_dir)
        .select(
            F.lit(None).cast("string").alias("message_key"),
            F.col("value").alias("raw_json"),
            F.lit(None).cast("string").alias("kafka_topic"),
            F.lit(None).cast("int").alias("kafka_partition"),
            F.lit(None).cast("long").alias("kafka_offset"),
            F.col("_metadata.file_modification_time").alias("enqueued_timestamp"),
        )
    )


def read_kafka(spark: SparkSession, config: LocalConfig) -> DataFrame:
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", config.kafka_topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "10000")
        .load()
    )
    return kafka_to_transport(kafka_df)


# ----------------------------------------------------------------------
# Stages
# ----------------------------------------------------------------------
def run_bronze(spark: SparkSession, tables: Tables, config: LocalConfig) -> None:
    source = read_kafka(spark, config) if config.source == "kafka" else read_landing_files(spark, config)
    _write_stream_to_table(
        parse_events(source),
        tables.bronze_events,
        config.checkpoint(f"bronze_order_events_{config.source}"),
    )


def run_silver(spark: SparkSession, tables: Tables, config: LocalConfig) -> None:
    def deduplicated_orders() -> DataFrame:
        return deduplicate_events(flatten_orders(spark.readStream.table(tables.bronze_events)))

    _write_stream_to_table(
        valid_orders(deduplicated_orders()).drop("raw_json", "dedup_key"),
        tables.silver_orders,
        config.checkpoint("silver_orders"),
    )
    _write_stream_to_table(
        invalid_orders(deduplicated_orders()).drop("dedup_key"),
        tables.silver_invalid_orders,
        config.checkpoint("silver_invalid_orders"),
    )


def run_cdf_merge(spark: SparkSession, tables: Tables) -> dict:
    return cdf_merge.run(spark, tables)


def run_gold(spark: SparkSession, tables: Tables, config: LocalConfig) -> None:
    _write_stream_to_table(
        metrics.order_activity_by_status(spark.readStream.table(tables.silver_orders)),
        tables.gold_order_activity,
        config.checkpoint("gold_order_activity"),
    )

    # The local equivalent of a materialized view: recompute and replace.
    current_orders = spark.table(tables.silver_current_orders)
    _replace_table_contents(spark, metrics.customer_metrics(current_orders), tables.gold_customer_metrics)
    _replace_table_contents(spark, metrics.product_metrics(current_orders), tables.gold_product_metrics)


def run_validation(spark: SparkSession, tables: Tables) -> list[dict]:
    return validate_outputs.run(spark, tables)


def layer_counts(spark: SparkSession, tables: Tables) -> dict[str, int]:
    """Row count of every table that exists (for the run summary)."""
    names = {
        "bronze.order_events": tables.bronze_events,
        "silver.orders": tables.silver_orders,
        "silver.invalid_orders": tables.silver_invalid_orders,
        "silver.current_orders": tables.silver_current_orders,
        "gold.order_activity_by_status": tables.gold_order_activity,
        "gold.customer_metrics": tables.gold_customer_metrics,
        "gold.product_metrics": tables.gold_product_metrics,
    }
    return {label: spark.table(name).count() for label, name in names.items() if spark.catalog.tableExists(name)}
