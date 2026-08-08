import logging
from datetime import datetime

from processing.spark_session import create_spark_session

from processing.transformers.gold_transformer import (
    create_sales_summary,
    create_customer_metrics,
    create_product_metrics,
)

from processing.writers.gold_writer import write_gold

from configs.config import (
    SILVER_PATH,
    GOLD_SALES_PATH,
    GOLD_CUSTOMER_PATH,
    GOLD_PRODUCT_PATH,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


def main():

    spark = None
    start_time = datetime.now()

    try:

        logger.info("=" * 70)
        logger.info("Starting Gold Layer Job")
        logger.info("=" * 70)

        spark = create_spark_session()

        logger.info("Reading Silver Layer...")

        silver_df = (
            spark.read
            .format("delta")
            .load(SILVER_PATH)
        )

        total_records = silver_df.count()

        logger.info(f"Total Silver Records : {total_records}")

        if total_records == 0:
            logger.warning("No records found in Silver Layer.")
            return

        # ======================================================
        # Transformations
        # ======================================================

        logger.info("Creating Sales Summary...")

        #In production we cache data before counting and writing to reduce disk jobs since it is expensive on large datasets

        sales_summary = create_sales_summary(silver_df).cache()

        logger.info("Creating Customer Metrics...")

        customer_metrics = create_customer_metrics(silver_df).cache()

        logger.info("Creating Product Metrics...")

        product_metrics = create_product_metrics(silver_df).cache()

        sales_rows = sales_summary.count()
        customer_rows = customer_metrics.count()
        product_rows = product_metrics.count()

        logger.info(f"Sales Summary Rows    : {sales_rows}")
        logger.info(f"Customer Metrics Rows : {customer_rows}")
        logger.info(f"Product Metrics Rows  : {product_rows}")

        # ======================================================
        # Gold Writes
        # ======================================================

        logger.info("Writing Sales Summary...")

        write_gold(
            spark=spark,
            df=sales_summary,
            target_path=GOLD_SALES_PATH,
            key_columns=["product_id"],
        )

        logger.info("Writing Customer Metrics...")

        write_gold(
            spark=spark,
            df=customer_metrics,
            target_path=GOLD_CUSTOMER_PATH,
            key_columns=["customer_id"],
        )

        logger.info("Writing Product Metrics...")

        write_gold(
            spark=spark,
            df=product_metrics,
            target_path=GOLD_PRODUCT_PATH,
            key_columns=["product_id"],
        )

        end_time = datetime.now()
        duration = end_time - start_time

        logger.info("=" * 70)
        logger.info("Gold Layer Job Completed Successfully")
        logger.info("=" * 70)
        logger.info(f"Started          : {start_time}")
        logger.info(f"Finished         : {end_time}")
        logger.info(f"Duration         : {duration}")
        logger.info(f"Silver Records   : {total_records}")
        logger.info(f"Sales Summary    : {sales_rows}")
        logger.info(f"Customer Metrics : {customer_rows}")
        logger.info(f"Product Metrics  : {product_rows}")
        logger.info("=" * 70)

    except Exception:

        logger.exception("Gold Layer Job Failed")

        raise

    finally:

        if spark is not None:
            spark.stop()
            logger.info("Spark Session Stopped")


if __name__ == "__main__":
    main()