"""Print what the pipeline produced: a quick tour of every layer.

python -m local_pipeline.show_results [--data-dir data]
"""

from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from local_pipeline import stages
from local_pipeline.config import LocalConfig
from local_pipeline.spark_session import create_spark_session
from shopsphere_data_platform_etl.core.tables import table_names


def show(title: str, df, rows: int = 10) -> None:
    print(f"\n=== {title} ===")
    df.show(rows, truncate=False)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Show the ShopSphere tables.")
    parser.add_argument("--data-dir", default="data")
    args = parser.parse_args(argv)

    spark = create_spark_session(LocalConfig(data_dir=args.data_dir))
    tables = table_names()
    try:
        print("\n=== Row counts ===")
        for label, count in stages.layer_counts(spark, tables).items():
            print(f"  {label:<32} {count}")

        show(
            "Quarantine: why records were rejected (silver.invalid_orders)",
            spark.table(tables.silver_invalid_orders).groupBy("validation_errors").count().orderBy(F.desc("count")),
        )
        show(
            "Current state of orders, by status (silver.current_orders)",
            spark.table(tables.silver_current_orders).groupBy("order_status").count().orderBy(F.desc("count")),
        )
        show(
            "One order's history (silver.orders) ...",
            spark.table(tables.silver_orders)
            .join(
                spark.table(tables.silver_orders).groupBy("order_id").count().orderBy(F.desc("count")).limit(1),
                "order_id",
            )
            .select("order_id", "event_type", "order_status", "event_timestamp", "is_late_event")
            .orderBy("event_timestamp"),
        )
        show(
            "Top customers by lifetime value (gold.customer_metrics)",
            spark.table(tables.gold_customer_metrics).orderBy(F.desc("lifetime_value")),
            5,
        )
        show(
            "Top products by revenue (gold.product_metrics)",
            spark.table(tables.gold_product_metrics).orderBy(F.desc("total_revenue")),
            5,
        )
        show(
            "Latest per-minute activity (gold.order_activity_by_status)",
            spark.table(tables.gold_order_activity).orderBy(F.desc("window_start"), "order_status"),
            8,
        )
        if spark.catalog.tableExists(tables.pipeline_state):
            show("CDF merge position (metadata.pipeline_state)", spark.table(tables.pipeline_state))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
