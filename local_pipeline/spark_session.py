"""Spark session for the local pipeline."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from local_pipeline.config import DELTA_PACKAGE, SPARK_KAFKA_PACKAGE, LocalConfig


def create_spark_session(config: LocalConfig, delta: bool = True) -> SparkSession:
    """Build a local session that behaves like the Databricks one where it matters.

    * Tables are Delta tables by default, as on Databricks.
    * Table names are kept in a small embedded metastore inside the data
      folder, so ``silver.orders`` still exists the next time the pipeline
      starts.
    * Every new table gets Change Data Feed, which the CDF merge job needs.

    ``delta=False`` gives a plain Parquet session. It exists so the fast tests
    can run the streaming stages without downloading the Delta libraries.
    """
    os.makedirs(config.root, exist_ok=True)

    packages = []
    if delta:
        packages.append(DELTA_PACKAGE)
    if config.source == "kafka":
        packages.append(SPARK_KAFKA_PACKAGE)

    builder = (
        SparkSession.builder.appName("ShopSphere local pipeline")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", config.warehouse_dir)
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={config.metastore_dir};create=true",
        )
        .enableHiveSupport()
    )
    if packages:
        # Downloaded from Maven Central on first use, then cached in ~/.ivy2.
        builder = builder.config("spark.jars.packages", ",".join(packages))
    if delta:
        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.sources.default", "delta")
            .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


if __name__ == "__main__":
    # Used while building the Docker image: starting a session once makes
    # Spark download its libraries, so containers start without waiting.
    create_spark_session(LocalConfig(data_dir="/tmp/shopsphere-warmup", source="kafka")).stop()
