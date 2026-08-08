from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


SOURCE_TABLE = "adb_shopsphere_dev.silver.orders"
TARGET_TABLE = "adb_shopsphere_dev.silver.current_orders"
STATE_TABLE = "adb_shopsphere_dev.metadata.pipeline_state"

PIPELINE_NAME = "current_orders_merge"

# CDF was enabled on silver.orders starting at version 9.
CDF_START_VERSION = 9


def main():
    spark = SparkSession.builder.getOrCreate()

    # ---------------------------------------------------------
    # 1. Find the latest available Delta version
    # ---------------------------------------------------------
    latest_version = (
        spark.sql(f"DESCRIBE HISTORY {SOURCE_TABLE}")
        .agg(F.max("version").alias("version"))
        .first()["version"]
    )

    print(f"Latest source version: {latest_version}")

    # ---------------------------------------------------------
    # 2. Read our previously processed version
    # ---------------------------------------------------------
    state_rows = (
        spark.table(STATE_TABLE)
        .filter(F.col("pipeline_name") == PIPELINE_NAME)
        .select("last_processed_version")
        .limit(1)
        .collect()
    )

    if state_rows:
        last_processed_version = state_rows[0]["last_processed_version"]
        starting_version = last_processed_version + 1

        print(
            f"Last processed version: "
            f"{last_processed_version}"
        )
    else:
        starting_version = CDF_START_VERSION

        print(
            "No previous state found. "
            f"Starting from CDF version {CDF_START_VERSION}."
        )

    # ---------------------------------------------------------
    # 3. Nothing new? Exit successfully.
    # ---------------------------------------------------------
    if starting_version > latest_version:
        print(
            "No new Delta versions to process. "
            "Exiting successfully."
        )
        return

    print(
        f"Processing CDF versions "
        f"{starting_version} → {latest_version}"
    )

    # ---------------------------------------------------------
    # 4. Read ONLY new CDF changes
    # ---------------------------------------------------------
    changes = (
        spark.read
        .option("readChangeFeed", "true")
        .option("startingVersion", starting_version)
        .option("endingVersion", latest_version)
        .table(SOURCE_TABLE)
        .filter(
            F.col("_change_type").isin(
                "insert",
                "update_postimage",
            )
        )
    )

    # ---------------------------------------------------------
    # 5. Keep latest change for each order in this batch
    # ---------------------------------------------------------
    window = (
        Window
        .partitionBy("order_id")
        .orderBy(
            F.col("_commit_version").desc(),
            F.col("event_timestamp").desc(),
        )
    )

    latest_changes = (
        changes
        .withColumn(
            "_row_number",
            F.row_number().over(window),
        )
        .filter(F.col("_row_number") == 1)
        .drop(
            "_row_number",
            "_change_type",
            "_commit_version",
            "_commit_timestamp",
        )
    )

    # ---------------------------------------------------------
    # 6. MERGE into current state
    # ---------------------------------------------------------
    if not spark.catalog.tableExists(TARGET_TABLE):

        (
            latest_changes.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(TARGET_TABLE)
        )

        print(f"Created {TARGET_TABLE}")

    else:

        target = DeltaTable.forName(
            spark,
            TARGET_TABLE,
        )

        (
            target.alias("target")
            .merge(
                latest_changes.alias("source"),
                "target.order_id = source.order_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print("MERGE completed.")

    # ---------------------------------------------------------
    # 7. Save successful processing state
    # ---------------------------------------------------------
    state_update = spark.createDataFrame(
        [
            (
                PIPELINE_NAME,
                int(latest_version),
            )
        ],
        [
            "pipeline_name",
            "last_processed_version",
        ],
    ).withColumn(
        "updated_at",
        F.current_timestamp(),
    )

    state_target = DeltaTable.forName(
        spark,
        STATE_TABLE,
    )

    (
        state_target.alias("target")
        .merge(
            state_update.alias("source"),
            "target.pipeline_name = source.pipeline_name",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(
        f"Saved checkpoint version: "
        f"{latest_version}"
    )


if __name__ == "__main__":
    main()