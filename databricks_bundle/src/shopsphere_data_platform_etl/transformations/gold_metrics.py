# Gold is optimized for consumption, not event-level processing.
# The Gold layer contains analytics-ready aggregates built from validated Silver orders. I use a streaming, 
# event-time windowed aggregation for near-real-time revenue-by-category metrics, including revenue, order count
#  and units sold. For customer and product lifetime metrics, I use Lakeflow materialized views over the Silver 
# table, calculating KPIs such as lifetime value, average order value, product revenue, unique customers and 
# units sold. 
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("shopsphere.catalog")

SILVER_ORDERS_TABLE = (
    f"{CATALOG}.silver.orders"
)

CURRENT_ORDERS_TABLE = (
    f"{CATALOG}.silver.current_orders"
)

GOLD_ORDER_ACTIVITY_TABLE = (
    f"{CATALOG}.gold.order_activity_by_status"
)

GOLD_CUSTOMER_METRICS_TABLE = (
    f"{CATALOG}.gold.customer_metrics"
)

GOLD_PRODUCT_METRICS_TABLE = (
    f"{CATALOG}.gold.product_metrics"
)


# ========================================================================================
# Streaming Gold Order Activity table - This tells Lakeflow to maintain a streaming table.
# ========================================================================================

@dp.table(
    name=GOLD_ORDER_ACTIVITY_TABLE,
    comment=(
        "One-minute event-time operational metrics "
        "grouped by order status."
    ),
)
def order_activity_by_status():
    return (
        spark.readStream
        .table(SILVER_ORDERS_TABLE)
        .withWatermark(
            "event_timestamp",
            "5 minutes",
        )
        .groupBy(
            F.window(
                F.col("event_timestamp"),
                "1 minute",
            ),
            F.col("order_status"),
        )
        .agg(
            F.count("*").alias("event_count"),
            F.approx_count_distinct("order_id").alias(
                "unique_orders"
            ),  
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "order_status",
            "event_count",
            "unique_orders",
        )
    )

# =======================================================================================================
# Materialized Gold view - This is a batch-style declarative aggregation over the current Silver table.
# Customer lifetime metrics represent the current aggregate state over the entire dataset.
# =======================================================================================================

@dp.materialized_view(
    name=GOLD_CUSTOMER_METRICS_TABLE,
    comment="Lifetime order metrics grouped by customer.",
)
def customer_metrics():
    return (
        spark.read
        .table(CURRENT_ORDERS_TABLE)
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("total_orders"),
            F.round(
                F.sum("total_amount"),
                2,
            ).alias("lifetime_value"),
            F.round(
                F.avg("total_amount"),
                2,
            ).alias("average_order_value"),
            F.max("event_timestamp").alias("last_purchase_at"),
            F.countDistinct("product_id").alias(
                "unique_products_purchased"
            ),
        )
    )


# =========================================================================================================
# Materialized Gold view - This is a batch-style declarative aggregation over the current Silver table.

# =========================================================================================================

@dp.materialized_view(
    name=GOLD_PRODUCT_METRICS_TABLE,
    comment="Lifetime sales metrics grouped by product.",
)
def product_metrics():
    return (
        spark.read
        .table(CURRENT_ORDERS_TABLE)
        .groupBy(
            "product_id",
            "product_name",
            "category",
            "brand",
        )
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.round(
                F.sum("total_amount"),
                2,
            ).alias("total_revenue"),
            F.count("*").alias("total_orders"),
            F.countDistinct("customer_id").alias(
                "unique_customers"
            ),
            F.round(
                F.avg("unit_price"),
                2,
            ).alias("average_unit_price"),
        )
    )

