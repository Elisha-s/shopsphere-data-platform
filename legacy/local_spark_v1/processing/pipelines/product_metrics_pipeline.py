from processing.spark_session import create_spark_session

from processing.readers.silver_reader import read_silver_stream

from product_metrics_transformer import (
    build_product_metrics
)

from processing.writers.product_metrics_writer import (
    write_product_metrics
)


def main():

    spark = create_spark_session()

    silver_df = read_silver_stream(spark)

    product_df = build_product_metrics(silver_df)

    query = write_product_metrics(product_df)

    query.awaitTermination()


if __name__ == "__main__":
    main()