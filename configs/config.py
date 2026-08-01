from pathlib import Path

# ==========================================================
# Generator Configuration
# ==========================================================

NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 500

COUNTRY = "India"

MIN_ORDER_AMOUNT = 100.0
MAX_ORDER_AMOUNT = 5000.0

# ==========================================================
# Kafka Configuration
# ==========================================================

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "orders"
STARTING_OFFSETS = "latest"

# ==========================================================
# Spark Configuration
# ==========================================================

SPARK_APP_NAME = "ShopSphere Bronze Pipeline"
SPARK_MASTER = "local[*]"

# ==========================================================
# Project Paths
# ==========================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"

# ==========================================================
# Bronze Layer
# ==========================================================

BRONZE_PATH = str(DATA_DIR / "bronze" / "orders")

BRONZE_CHECKPOINT = str(
    DATA_DIR / "checkpoints" / "bronze" / "orders"
)

# ==========================================================
# Silver Layer
# ==========================================================

SILVER_PATH = str(
    DATA_DIR / "silver" / "orders"
)

SILVER_CHECKPOINT = str(
    DATA_DIR / "checkpoints" / "silver" / "orders"
)


# ==========================================================
# Gold Layer
# ==========================================================

GOLD_REVENUE_BY_CATEGORY_PATH = str(
    DATA_DIR / "gold" / "revenue_by_category"
)

GOLD_REVENUE_BY_CATEGORY_CHECKPOINT = str(
    DATA_DIR / "checkpoints" / "gold" / "revenue_by_category"
)


GOLD_REVENUE_BY_BRAND_PATH = str(
    DATA_DIR / "gold" / "revenue_by_brand"
)

GOLD_REVENUE_BY_BRAND_CHECKPOINT = str(
    DATA_DIR / "checkpoints" / "gold" / "revenue_by_brand"
)


GOLD_CUSTOMER_METRICS_PATH = str(
    DATA_DIR / "gold" / "customer_metrics"
)

GOLD_CUSTOMER_METRICS_CHECKPOINT = str(
    DATA_DIR / "checkpoints" / "gold" / "customer_metrics"
)


GOLD_PRODUCT_METRICS_PATH = str(
    DATA_DIR / "gold" / "product_metrics"
)

GOLD_PRODUCT_METRICS_CHECKPOINT = str(
    DATA_DIR / "checkpoints" / "gold" / "product_metrics"
)


GOLD_SALES_SUMMARY_PATH = str(
    DATA_DIR / "gold" / "sales_summary"
)

GOLD_SALES_SUMMARY_CHECKPOINT = str(
    DATA_DIR / "checkpoints" / "gold" / "sales_summary"
)


# ==========================================================
# Project Configuration
# ==========================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"

# ==========================================================
# Logging
# ==========================================================

LOG_LEVEL = "WARN"

INVALID_PATH = str(
    DATA_DIR / "invalid" / "orders"
)

INVALID_CHECKPOINT = str(
    DATA_DIR / "checkpoints" / "invalid_orders"
)