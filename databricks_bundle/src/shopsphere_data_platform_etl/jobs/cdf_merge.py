"""Incrementally maintain ``silver.current_orders`` from ``silver.orders``.

How it works
------------
1. ``silver.orders`` has Delta Change Data Feed (CDF) enabled, so every commit
   records which rows it added.
2. A one-row state table remembers the last Delta version this job processed.
3. Each run reads only the versions after that, keeps the newest event per
   order, and MERGEs it into ``current_orders``.
4. The state is advanced only after the MERGE has succeeded.

Why a retry is safe
-------------------
If the job dies between steps 3 and 4, the next run reads the same versions
again. The MERGE is keyed on ``order_id`` and only applies an event that is
newer than the stored one, so replaying a version range changes nothing.
"""

from __future__ import annotations

import argparse

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from shopsphere_data_platform_etl.core.cdc import (
    MERGE_KEY_CONDITION,
    SOURCE_IS_NEWER_CONDITION,
    latest_change_per_order,
)
from shopsphere_data_platform_etl.core.tables import Tables, table_names

PIPELINE_NAME = "current_orders_merge"


def ensure_state_table(spark: SparkSession, tables: Tables) -> None:
    """Create the state table on first use, so a fresh environment just works."""
    schema_name = tables.pipeline_state.rsplit(".", 1)[0]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {tables.pipeline_state} (
            pipeline_name STRING,
            last_processed_version BIGINT,
            updated_at TIMESTAMP
        ) USING DELTA
        """
    )


def read_last_processed_version(spark: SparkSession, tables: Tables) -> int | None:
    rows = (
        spark.table(tables.pipeline_state)
        .filter(F.col("pipeline_name") == PIPELINE_NAME)
        .select("last_processed_version")
        .limit(1)
        .collect()
    )
    return rows[0]["last_processed_version"] if rows else None


def save_last_processed_version(spark: SparkSession, tables: Tables, version: int) -> None:
    update = spark.createDataFrame(
        [(PIPELINE_NAME, int(version))],
        "pipeline_name STRING, last_processed_version BIGINT",
    ).withColumn("updated_at", F.current_timestamp())

    (
        DeltaTable.forName(spark, tables.pipeline_state)
        .alias("target")
        .merge(update.alias("source"), "target.pipeline_name = source.pipeline_name")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def run(spark: SparkSession, tables: Tables, initial_version: int = 0) -> dict:
    """Process every unprocessed version of silver.orders. Returns a summary."""
    ensure_state_table(spark, tables)

    latest_version = (
        spark.sql(f"DESCRIBE HISTORY {tables.silver_orders}").agg(F.max("version").alias("version")).first()["version"]
    )
    last_processed = read_last_processed_version(spark, tables)

    if last_processed is None:
        # First run in this environment. The Silver table is created with CDF
        # enabled, so its change feed is readable from its first version.
        starting_version = initial_version
    else:
        starting_version = last_processed + 1

    if last_processed is not None and last_processed > latest_version:
        # The saved position is ahead of the table. That only happens when the
        # source table was dropped and recreated (for example by a full
        # refresh). Stopping here is deliberate: carrying on would report
        # "nothing new" forever while current_orders went stale.
        raise RuntimeError(
            f"State says version {last_processed} of {tables.silver_orders} was processed, "
            f"but the table is only at version {latest_version}. The table was probably recreated. "
            f"Delete the '{PIPELINE_NAME}' row from {tables.pipeline_state} (and drop "
            f"{tables.silver_current_orders} if it should be rebuilt), then run again."
        )

    if starting_version > latest_version:
        print("No new Delta versions to process.")
        return {"status": "up_to_date", "latest_version": latest_version}

    print(f"Processing change feed versions {starting_version} to {latest_version}")

    changes = (
        spark.read.option("readChangeFeed", "true")
        .option("startingVersion", starting_version)
        .option("endingVersion", latest_version)
        .table(tables.silver_orders)
    )
    latest_changes = latest_change_per_order(changes)

    if not spark.catalog.tableExists(tables.silver_current_orders):
        latest_changes.write.format("delta").saveAsTable(tables.silver_current_orders)
        print(f"Created {tables.silver_current_orders}")
    else:
        (
            DeltaTable.forName(spark, tables.silver_current_orders)
            .alias("target")
            .merge(latest_changes.alias("source"), MERGE_KEY_CONDITION)
            .whenMatchedUpdateAll(condition=SOURCE_IS_NEWER_CONDITION)
            .whenNotMatchedInsertAll()
            .execute()
        )
        print("MERGE completed.")

    # Advance the saved position only now, after the data is safely merged.
    save_last_processed_version(spark, tables, latest_version)
    print(f"Saved processed version: {latest_version}")

    return {
        "status": "merged",
        "starting_version": starting_version,
        "latest_version": latest_version,
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Merge new silver.orders changes into silver.current_orders.")
    parser.add_argument("--catalog", default=None, help="Unity Catalog name. Omit for a local Spark session.")
    parser.add_argument(
        "--initial-version",
        type=int,
        default=0,
        help="Delta version to start from when no state exists yet.",
    )
    args = parser.parse_args(argv)

    spark = SparkSession.builder.getOrCreate()
    run(spark, table_names(args.catalog), initial_version=args.initial_version)


if __name__ == "__main__":
    main()
