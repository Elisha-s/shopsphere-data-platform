"""End-of-run reconciliation: prove the layers agree with each other.

Every check compares two numbers that must be equal if the pipeline is
correct. All checks run, then the job fails with the full list of problems,
so one run reports everything that is wrong rather than only the first thing.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from shopsphere_data_platform_etl.core.metrics import revenue_orders
from shopsphere_data_platform_etl.core.orders import dedup_key
from shopsphere_data_platform_etl.core.tables import Tables, table_names


class ValidationError(RuntimeError):
    pass


def collect_checks(spark: SparkSession, tables: Tables) -> list[dict]:
    """Run every check and return one result dictionary per check."""
    bronze = spark.table(tables.bronze_events)
    silver = spark.table(tables.silver_orders)
    invalid = spark.table(tables.silver_invalid_orders)
    current = spark.table(tables.silver_current_orders)
    customer_metrics = spark.table(tables.gold_customer_metrics)
    product_metrics = spark.table(tables.gold_product_metrics)

    checks: list[dict] = []

    def check(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    # 1. Nothing lost, nothing counted twice between Bronze and Silver:
    #    every distinct event is either a valid order or a quarantined one.
    bronze_rows = bronze.count()
    bronze_unique = bronze.select(dedup_key().alias("dedup_key")).distinct().count()
    silver_rows = silver.count()
    invalid_rows = invalid.count()
    check(
        "bronze_equals_silver_plus_invalid",
        bronze_unique == silver_rows + invalid_rows,
        f"bronze rows={bronze_rows}, unique events={bronze_unique} "
        f"(duplicates removed={bronze_rows - bronze_unique}), "
        f"silver valid={silver_rows}, silver invalid={invalid_rows}",
    )

    # 2. current_orders has exactly one row per order seen in Silver.
    current_rows = current.count()
    current_distinct_orders = current.select("order_id").distinct().count()
    silver_distinct_orders = silver.select("order_id").distinct().count()
    check(
        "current_orders_one_row_per_order",
        current_rows == current_distinct_orders == silver_distinct_orders,
        f"current_orders rows={current_rows}, distinct order ids={current_distinct_orders}, "
        f"distinct order ids in silver={silver_distinct_orders}",
    )

    # 3. Each order's stored state is its newest event, even when events
    #    arrived out of order.
    newest_per_order = silver.groupBy("order_id").agg(F.max("event_timestamp").alias("newest_event_timestamp"))
    stale_orders = (
        current.join(newest_per_order, "order_id")
        .filter(F.col("event_timestamp") < F.col("newest_event_timestamp"))
        .count()
    )
    check(
        "current_orders_hold_latest_state",
        stale_orders == 0,
        f"orders whose stored state is older than their newest event={stale_orders}",
    )

    # 4. Gold totals equal the totals of the table they are built from.
    #    (Each Gold row is rounded to 2 decimals, hence the small tolerance.)
    revenue = revenue_orders(current)
    expected_revenue = revenue.agg(F.sum("total_amount")).first()[0] or 0.0
    expected_orders = revenue.count()

    customer_rows = customer_metrics.count()
    customer_revenue = customer_metrics.agg(F.sum("lifetime_value")).first()[0] or 0.0
    customer_orders = customer_metrics.agg(F.sum("total_orders")).first()[0] or 0
    check(
        "customer_metrics_match_current_orders",
        customer_orders == expected_orders and abs(customer_revenue - expected_revenue) <= 0.01 * max(customer_rows, 1),
        f"orders: gold={customer_orders} expected={expected_orders}; "
        f"revenue: gold={customer_revenue:.2f} expected={expected_revenue:.2f}",
    )

    product_rows = product_metrics.count()
    product_revenue = product_metrics.agg(F.sum("total_revenue")).first()[0] or 0.0
    product_orders = product_metrics.agg(F.sum("total_orders")).first()[0] or 0
    check(
        "product_metrics_match_current_orders",
        product_orders == expected_orders and abs(product_revenue - expected_revenue) <= 0.01 * max(product_rows, 1),
        f"orders: gold={product_orders} expected={expected_orders}; "
        f"revenue: gold={product_revenue:.2f} expected={expected_revenue:.2f}",
    )

    # 5. The pipeline produced something at all.
    check("current_orders_not_empty", current_rows > 0, f"rows={current_rows}")

    return checks


def run(spark: SparkSession, tables: Tables) -> list[dict]:
    checks = collect_checks(spark, tables)

    print("ShopSphere validation")
    for result in checks:
        print(f"  [{'PASS' if result['passed'] else 'FAIL'}] {result['name']}: {result['detail']}")

    # Informational only: the per-minute activity table is filled by a
    # watermarked streaming aggregation, which holds a window back until the
    # watermark passes it. It is legitimately empty for the first few minutes
    # of data, so emptiness is reported but is not a failure.
    if spark.catalog.tableExists(tables.gold_order_activity):
        activity_rows = spark.table(tables.gold_order_activity).count()
        print(f"  [INFO] order_activity_by_status rows={activity_rows}")

    failed = [result for result in checks if not result["passed"]]
    if failed:
        summary = "; ".join(f"{result['name']} ({result['detail']})" for result in failed)
        raise ValidationError(f"{len(failed)} validation check(s) failed: {summary}")

    print("ShopSphere validation passed.")
    return checks


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Reconcile the ShopSphere layers.")
    parser.add_argument("--catalog", default=None, help="Unity Catalog name. Omit for a local Spark session.")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.getOrCreate()
    run(spark, table_names(args.catalog))


if __name__ == "__main__":
    main()
