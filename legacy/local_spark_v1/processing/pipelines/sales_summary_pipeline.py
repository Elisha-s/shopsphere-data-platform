from processing.spark_session import create_spark_session

from processing.readers.silver_reader import read_silver_stream

from sales_summary_transformer import (
    sales_summary
)

from processing.writers.sales_summary_writer import (
    write_sales_summary
)


def main():

    spark = create_spark_session()

    silver_df = read_silver_stream(spark)

    summary_df = sales_summary(silver_df)

    query = write_sales_summary(summary_df)

    query.awaitTermination()


if __name__ == "__main__":
    main()