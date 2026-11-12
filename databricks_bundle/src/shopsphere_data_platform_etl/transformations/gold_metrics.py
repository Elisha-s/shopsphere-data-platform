"""Gold: analytics-ready tables.

* ``order_activity_by_status`` is a streaming table: per-minute event counts,
  computed incrementally from the Silver event stream.
* ``customer_metrics`` and ``product_metrics`` are materialized views over
  ``silver.current_orders`` (one row per order), so an order is counted once
  no matter how many status events it produced.

``silver.current_orders`` is written by the CDF merge job, not by this
pipeline, so that job must run between the Silver and Gold refreshes. The
ADF pipeline enforces that order.
"""

from pyspark import pipelines as dp

from shopsphere_data_platform_etl.core import metrics
from shopsphere_data_platform_etl.core.tables import table_names

TABLES = table_names(spark.conf.get("shopsphere.catalog"))


@dp.table(
    name=TABLES.gold_order_activity,
    comment="Order events per one-minute event-time window, by order status.",
)
def order_activity_by_status():
    return metrics.order_activity_by_status(spark.readStream.table(TABLES.silver_orders))


@dp.materialized_view(
    name=TABLES.gold_customer_metrics,
    comment="Lifetime order metrics per customer (paid, shipped or delivered orders).",
)
def customer_metrics():
    return metrics.customer_metrics(spark.read.table(TABLES.silver_current_orders))


@dp.materialized_view(
    name=TABLES.gold_product_metrics,
    comment="Lifetime sales metrics per product (paid, shipped or delivered orders).",
)
def product_metrics():
    return metrics.product_metrics(spark.read.table(TABLES.silver_current_orders))
