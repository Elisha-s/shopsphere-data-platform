"""Runs the streaming stages for real, on a plain (non-Delta) Spark session.

Covers everything except the Delta-specific CDF merge job, whose "latest
event per order" result is stood in for with a batch computation here. The
full pipeline, including the merge, is exercised by tests/integration.
"""

from pyspark.sql import functions as F

from local_pipeline import stages
from local_pipeline.config import LocalConfig
from shopsphere_data_platform_etl.core.cdc import latest_change_per_order
from shopsphere_data_platform_etl.core.tables import table_names
from tests.helpers import generate_landing_files


def test_stages_reconcile_with_what_the_generator_injected(spark, tmp_path):
    config = LocalConfig(data_dir=str(tmp_path), source="files")
    tables = table_names()
    stats = generate_landing_files(config.landing_dir, ticks=1500, seed=11)

    stages.ensure_schemas(spark, tables)
    stages.run_bronze(spark, tables, config)
    stages.run_silver(spark, tables, config)

    bronze = spark.table(tables.bronze_events)
    silver = spark.table(tables.silver_orders)
    invalid = spark.table(tables.silver_invalid_orders)

    # Bronze has every delivered message, duplicates included.
    assert bronze.count() == stats["events_delivered"]
    # Silver removed exactly the duplicates the generator injected ...
    assert silver.count() + invalid.count() == stats["events_delivered"] - stats["duplicates"]
    # ... and quarantined exactly the invalid events, each with a reason and its raw JSON.
    assert invalid.count() == stats["invalid"]
    assert invalid.filter("validation_errors = '' OR raw_json IS NULL").count() == 0
    assert silver.count() > 0 and stats["invalid"] > 0 and stats["duplicates"] > 0 and stats["late"] > 0

    # Running the stages again processes nothing new (checkpoints).
    stages.run_bronze(spark, tables, config)
    stages.run_silver(spark, tables, config)
    assert spark.table(tables.bronze_events).count() == stats["events_delivered"]
    assert spark.table(tables.silver_orders).count() + spark.table(tables.silver_invalid_orders).count() == (
        stats["events_delivered"] - stats["duplicates"]
    )

    # Stand-in for the CDF merge job: latest event per order.
    as_changes = (
        silver.withColumn("_change_type", F.lit("insert"))
        .withColumn("_commit_version", F.lit(1))
        .withColumn("_commit_timestamp", F.current_timestamp())
    )
    latest_change_per_order(as_changes).write.mode("overwrite").saveAsTable(tables.silver_current_orders)

    stages.run_gold(spark, tables, config)
    stages.run_gold(spark, tables, config)  # second refresh replaces the rows of the existing tables
    checks = stages.run_validation(spark, tables)  # raises if any check fails
    assert all(check["passed"] for check in checks)

    # The backfill spans two hours of event time, so per-minute windows older
    # than the 10-minute lateness allowance have been emitted.
    assert spark.table(tables.gold_order_activity).count() > 0
