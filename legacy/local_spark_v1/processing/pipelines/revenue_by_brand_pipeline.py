from processing.spark_session import create_spark_session

from processing.readers.silver_reader import read_silver_stream

from revenue_by_brand_transformer import (
    revenue_by_brand
)

from processing.writers.revenue_by_brand_writer import (
    write_revenue_by_brand
)


def main():

    spark = create_spark_session()

    silver_df = read_silver_stream(spark)

    revenue_df = revenue_by_brand(silver_df)

    query = write_revenue_by_brand(revenue_df)

    query.awaitTermination()


if __name__ == "__main__":
    main()