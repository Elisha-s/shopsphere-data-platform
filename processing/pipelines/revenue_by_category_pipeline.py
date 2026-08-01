from processing.spark_session import create_spark_session

from processing.readers.silver_reader import read_silver_stream

from revenue_by_category_transformer import (
    revenue_by_category
)

from processing.writers.revenue_by_category_writer import (
    write_revenue_by_category
)

from pyspark.sql.functions import col, window, sum


def main():

    spark = create_spark_session()

    silver_df = read_silver_stream(spark)

    revenue_df = revenue_by_category(silver_df)

    query = write_revenue_by_category(revenue_df)

    query.awaitTermination()


if __name__ == "__main__":
    main()