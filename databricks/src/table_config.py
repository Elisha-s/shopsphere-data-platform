"""Unity Catalog table configuration for ShopSphere."""

CATALOG = "adb_shopsphere_dev"

BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"
MONITORING_SCHEMA = f"{CATALOG}.monitoring"

BRONZE_ORDER_EVENTS_TABLE = f"{BRONZE_SCHEMA}.order_events"

SILVER_ORDERS_TABLE = f"{SILVER_SCHEMA}.orders"
INVALID_ORDERS_TABLE = f"{SILVER_SCHEMA}.invalid_orders"

GOLD_REVENUE_BY_CATEGORY_TABLE = (
    f"{GOLD_SCHEMA}.revenue_by_category"
)

GOLD_REVENUE_BY_BRAND_TABLE = (
    f"{GOLD_SCHEMA}.revenue_by_brand"
)

GOLD_CUSTOMER_METRICS_TABLE = (
    f"{GOLD_SCHEMA}.customer_metrics"
)

GOLD_PRODUCT_METRICS_TABLE = (
    f"{GOLD_SCHEMA}.product_metrics"
)

GOLD_SALES_SUMMARY_TABLE = (
    f"{GOLD_SCHEMA}.sales_summary"
)