"""Gold-layer aggregations."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# An order counts as revenue once it has been paid for. CREATED orders may
# never be paid and CANCELLED ones never will be.
REVENUE_STATUSES = ("PAID", "SHIPPED", "DELIVERED")

ACTIVITY_WINDOW = "1 minute"
# How long a one-minute window stays open for late events. The generator
# delivers its late events 7 minutes late, so 10 minutes includes them;
# anything later than that is intentionally left out of this operational view.
ACTIVITY_LATENESS = "10 minutes"


def revenue_orders(current_orders: DataFrame) -> DataFrame:
    return current_orders.filter(F.col("order_status").isin(*REVENUE_STATUSES))


def order_activity_by_status(
    silver_orders_stream: DataFrame,
    window: str = ACTIVITY_WINDOW,
    lateness: str = ACTIVITY_LATENESS,
) -> DataFrame:
    """Per-minute count of order events by status (streaming, event time).

    A window's row is emitted once, when the watermark passes the end of the
    window, so the newest ~10 minutes of activity are not in the table yet.
    """
    return (
        silver_orders_stream.withWatermark("event_timestamp", lateness)
        .groupBy(F.window(F.col("event_timestamp"), window), F.col("order_status"))
        .agg(
            F.count("*").alias("event_count"),
            F.approx_count_distinct("order_id").alias("unique_orders"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "order_status",
            "event_count",
            "unique_orders",
        )
    )


def customer_metrics(current_orders: DataFrame) -> DataFrame:
    """Lifetime value per customer, from the current state of each order."""
    return (
        revenue_orders(current_orders)
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("total_orders"),
            F.round(F.sum("total_amount"), 2).alias("lifetime_value"),
            F.round(F.avg("total_amount"), 2).alias("average_order_value"),
            F.max("order_timestamp").alias("last_purchase_at"),
            F.countDistinct("product_id").alias("unique_products_purchased"),
        )
    )


def product_metrics(current_orders: DataFrame) -> DataFrame:
    """Sales per product, from the current state of each order."""
    return (
        revenue_orders(current_orders)
        .groupBy("product_id", "product_name", "category", "brand")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.count("*").alias("total_orders"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.round(F.avg("unit_price"), 2).alias("average_unit_price"),
        )
    )
