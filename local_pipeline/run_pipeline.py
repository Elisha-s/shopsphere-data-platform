"""Run the ShopSphere pipeline locally, end to end.

The stage order is the same one Azure Data Factory enforces in the cloud:

    Bronze -> Silver -> CDF merge -> Gold -> Validation

Examples
--------
Process whatever is in the landing folder once:

    python -m local_pipeline.run_pipeline --source files

Keep processing a Kafka topic every 30 seconds:

    python -m local_pipeline.run_pipeline --source kafka --loop 30
"""

from __future__ import annotations

import argparse
import logging
import shutil
import time

from local_pipeline import stages
from local_pipeline.config import LocalConfig
from local_pipeline.spark_session import create_spark_session
from shopsphere_data_platform_etl.core.tables import table_names

logger = logging.getLogger("shopsphere.pipeline")


def run_once(spark, config: LocalConfig) -> dict[str, int]:
    tables = table_names()  # no catalog locally: bronze.order_events, silver.orders, ...
    stages.ensure_schemas(spark, tables)

    steps = [
        ("Bronze: ingest raw events", lambda: stages.run_bronze(spark, tables, config)),
        ("Silver: flatten, de-duplicate, validate", lambda: stages.run_silver(spark, tables, config)),
        ("CDF merge: update current_orders", lambda: stages.run_cdf_merge(spark, tables)),
        ("Gold: refresh metrics", lambda: stages.run_gold(spark, tables, config)),
        ("Validation: reconcile the layers", lambda: stages.run_validation(spark, tables)),
    ]
    for number, (title, step) in enumerate(steps, start=1):
        started = time.time()
        logger.info("[%s/%s] %s", number, len(steps), title)
        step()
        logger.info("[%s/%s] done in %.1fs", number, len(steps), time.time() - started)

    counts = stages.layer_counts(spark, tables)
    logger.info("Row counts after this run:")
    for label, count in counts.items():
        logger.info("  %-32s %s", label, count)
    return counts


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the ShopSphere pipeline on local Spark + Delta.")
    parser.add_argument("--source", choices=["files", "kafka"], default="files")
    parser.add_argument("--data-dir", default="data", help="Folder for landing files, tables and checkpoints.")
    parser.add_argument("--bootstrap-servers", default="localhost:9094")
    parser.add_argument("--topic", default="orders")
    parser.add_argument("--loop", type=int, default=None, metavar="SECONDS", help="Run again every N seconds.")
    parser.add_argument("--reset", action="store_true", help="Delete the tables and checkpoints before running.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    args = parse_args(argv)
    config = LocalConfig(
        data_dir=args.data_dir,
        source=args.source,
        kafka_bootstrap_servers=args.bootstrap_servers,
        kafka_topic=args.topic,
    )

    if args.reset:
        # Landing files are kept: they are the input, and can be replayed.
        for path in (config.warehouse_dir, config.metastore_dir, config.checkpoint("")):
            shutil.rmtree(path, ignore_errors=True)
        logger.info("Reset: removed tables, metastore and checkpoints under %s", config.root)

    spark = create_spark_session(config)
    try:
        if args.loop is None:
            run_once(spark, config)
            return
        while True:
            try:
                run_once(spark, config)
            except Exception:
                # In loop mode one bad cycle (Kafka not up yet, a failed
                # validation) is logged and the next cycle still runs.
                logger.exception("Pipeline cycle failed")
            time.sleep(args.loop)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
