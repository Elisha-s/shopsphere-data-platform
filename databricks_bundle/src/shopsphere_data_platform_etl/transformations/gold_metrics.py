from pyspark import pipelines as dp
from pyspark.sql import functions as F


CATALOG = spark.conf.get("shopsphere.catalog")

SILVER_ORDERS_TABLE = f"{CATALOG}.silver.orders"

GOLD_REVENUE_BY_CATEGORY_TABLE = (
    f"{CATALOG}.gold.revenue_by_category"
)

GOLD_CUSTOMER_METRICS_TABLE = (
    f"{CATALOG}.gold.customer_metrics"
)

GOLD_PRODUCT_METRICS_TABLE = (
    f"{CATALOG}.gold.product_metrics"
)


# ============================================================
# Streaming Gold table
# ============================================================

@dp.table(
    name=GOLD_REVENUE_BY_CATEGORY_TABLE,
    comment=(
        "One-minute event-time revenue and order metrics "
        "grouped by product category."
    ),
)
def revenue_by_category():
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
            F.col("category"),
        )
        .agg(
            F.round(
                F.sum("total_amount"),
                2,
            ).alias("total_revenue"),
            F.count("*").alias("total_orders"),
            F.sum("quantity").alias("units_sold"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "category",
            "total_revenue",
            "total_orders",
            "units_sold",
        )
    )


# ============================================================
# Materialized Gold view
# ============================================================

@dp.materialized_view(
    name=GOLD_CUSTOMER_METRICS_TABLE,
    comment="Lifetime order metrics grouped by customer.",
)
def customer_metrics():
    return (
        spark.read
        .table(SILVER_ORDERS_TABLE)
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


# ============================================================
# Materialized Gold view
# ============================================================

@dp.materialized_view(
    name=GOLD_PRODUCT_METRICS_TABLE,
    comment="Lifetime sales metrics grouped by product.",
)
def product_metrics():
    return (
        spark.read
        .table(SILVER_ORDERS_TABLE)
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