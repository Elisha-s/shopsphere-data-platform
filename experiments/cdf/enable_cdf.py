from processing.spark_session import create_spark_session
from configs.config import SILVER_PATH

spark = create_spark_session()

# Remove old catalog entry if it exists
spark.sql("DROP TABLE IF EXISTS silver_orders")

# Register your existing Delta table
spark.sql(f"""
CREATE TABLE silver_orders
USING DELTA
LOCATION '{SILVER_PATH}'
""")

# Enable Change Data Feed
spark.sql("""
ALTER TABLE silver_orders
SET TBLPROPERTIES (
  delta.enableChangeDataFeed = true
)
""")

# Verify
spark.sql("SHOW TBLPROPERTIES silver_orders").show(truncate=False)

spark.stop()