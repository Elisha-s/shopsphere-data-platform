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

KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
KAFKA_TOPIC = "orders"
STARTING_OFFSETS = "earliest"

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

GOLD_TOTAL_REVENUE_PATH = str(
    DATA_DIR / "gold" / "total_revenue"
)

GOLD_TOTAL_REVENUE_CHECKPOINT = str(
    DATA_DIR / "checkpoints" / "gold" / "total_revenue"
)

GOLD_CUSTOMER_REVENUE_PATH = str(
    DATA_DIR / "gold" / "customer_revenue"
)

GOLD_CUSTOMER_REVENUE_CHECKPOINT = str(
    DATA_DIR / "checkpoints" / "gold" / "customer_revenue"
)

GOLD_ORDERS_PER_PRODUCT_PATH = str(
    DATA_DIR / "gold" / "orders_per_product"
)

GOLD_ORDERS_PER_PRODUCT_CHECKPOINT = str(
    DATA_DIR / "checkpoints" / "gold" / "orders_per_product"
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