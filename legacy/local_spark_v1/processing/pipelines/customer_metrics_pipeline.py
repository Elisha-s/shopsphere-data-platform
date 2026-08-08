from processing.spark_session import create_spark_session

from processing.readers.silver_reader import read_silver_stream

from customer_metrics_transformer import (
    build_customer_metrics
)

from processing.writers.customer_metrics_writer import (
    write_customer_metrics
)


def main():

    spark = create_spark_session()

    silver_df = read_silver_stream(spark)

    customer_df = build_customer_metrics(silver_df)

    query = write_customer_metrics(customer_df)

    query.awaitTermination()


if __name__ == "__main__":
    main()