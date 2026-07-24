from processing.spark_session import create_spark_session

from processing.transformers.gold_transformer import (
    calculate_total_revenue,
    calculate_customer_revenue,
    calculate_product_revenue,
    build_customer_metrics,
    build_product_metrics,
    build_sales_summary
)

from processing.writers.gold_writer import write_gold
from processing.logger import get_logger

logger = get_logger(__name__)

from configs.config import (
    SILVER_PATH,
    GOLD_TOTAL_REVENUE_PATH,
    GOLD_CUSTOMER_REVENUE_PATH,
    GOLD_PRODUCT_REVENUE_PATH,
    GOLD_SALES_SUMMARY_PATH,
    GOLD_CUSTOMER_METRICS_PATH,
    GOLD_PRODUCT_METRICS_PATH
)

def main():

    try :
        #create spark session
        spark = create_spark_session()

        print("Reading Silver Layer..")

        silver_df = spark.read.format("delta").load(SILVER_PATH)

        print(f"total silver records :{silver_df.count()}")

        sales_summary = build_sales_summary(silver_df)
        customer_metrics = build_customer_metrics(silver_df)
        product_metrics = build_product_metrics(silver_df)

        total_revenue = calculate_total_revenue(silver_df)
        customer_revenue = calculate_customer_revenue(silver_df)
        product_revenue = calculate_product_revenue(silver_df)

        logger.info("Writing Sales Summary...")

        write_gold(
            sales_summary,
            GOLD_SALES_SUMMARY_PATH,
        )

        logger.info("Writing Customer Metrics...")

        write_gold(
            customer_metrics,
            GOLD_CUSTOMER_METRICS_PATH,
        )

        logger.info("Writing Product Metrics...")

        write_gold(
            product_metrics,
            GOLD_PRODUCT_METRICS_PATH,
        )

        total_revenue.write.format("delta").mode("overwrite").save(GOLD_TOTAL_REVENUE_PATH)
        customer_revenue.write.format("delta").mode("overwrite").save(GOLD_CUSTOMER_REVENUE_PATH)
        product_revenue.write.format("delta").mode("overwrite").save(GOLD_PRODUCT_REVENUE_PATH)

        logger.info("Gold Layer refreshed successfully.")

    except Exception:
        logger.info("Gold Layer failed")
        raise

    spark.stop()

if __name__ == "__main__":
    main()