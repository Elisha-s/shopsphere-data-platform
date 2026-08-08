from processing.spark_session import create_spark_session

from processing.readers.silver_reader import read_silver_stream

from processing.transformers.revenue_by_category_transformer import revenue_by_category
from processing.transformers.revenue_by_brand_transformer import revenue_by_brand
from processing.transformers.customer_metrics_transformer import build_customer_metrics
from processing.transformers.product_metrics_transformer import build_product_metrics
from processing.transformers.sales_summary_transformer import sales_summary

from processing.writers.revenue_by_category_writer import write_revenue_by_category
from processing.writers.revenue_by_brand_writer import write_revenue_by_brand
from processing.writers.customer_metrics_writer import write_customer_metrics
from processing.writers.product_metrics_writer import write_product_metrics
from processing.writers.sales_summary_writer import write_sales_summary

from processing.logger import get_logger

import time

logger = get_logger(__name__)


def main():

    spark = create_spark_session()

    logger.info("Reading Silver stream...")

    silver_stream = read_silver_stream(spark)

    logger.info("Building Gold datasets...")

    revenue_category_df = revenue_by_category(silver_stream)

    revenue_brand_df = revenue_by_brand(silver_stream)

    customer_metrics_df = build_customer_metrics(silver_stream)

    product_metrics_df = build_product_metrics(silver_stream)

    sales_summary_df = sales_summary(silver_stream)

    logger.info("Starting Gold writers...")

    revenue_category_query = write_revenue_by_category(
        revenue_category_df
    )

    revenue_brand_query = write_revenue_by_brand(
        revenue_brand_df
    )

    customer_query = write_customer_metrics(
        customer_metrics_df
    )

    product_query = write_product_metrics(
        product_metrics_df
    )

    sales_query = write_sales_summary(
        sales_summary_df
    )

    while True:

        print("\n========== GOLD ==========")

        print("\nRevenue Category")
        print(revenue_category_query.status)

        print("\nRevenue Brand")
        print(revenue_brand_query.status)

        print("\nCustomer Metrics")
        print(customer_query.status)

        print("\nProduct Metrics")
        print(product_query.status)

        print("\nSales Summary")
        print(sales_query.status)

        time.sleep(5)


if __name__ == "__main__":
    main()