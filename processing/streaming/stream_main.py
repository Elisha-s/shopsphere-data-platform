from processing.spark_session import create_spark_session

from processing.readers.kafka_reader import create_kafka_stream
from processing.transformers.parser import parse_orders

from processing.writers.bronze_writer import write_bronze
from processing.writers.silver_writer import write_silver
from processing.transformers.silver_transformer import clean_orders



def main():

    spark = create_spark_session()

    raw_stream = create_kafka_stream(spark)

    parsed_stream = parse_orders(raw_stream)

    bronze_query = write_bronze(parsed_stream)

    silver_df = clean_orders(parsed_stream)

    silver_query = write_silver(silver_df)

    bronze_query.awaitTermination()
    silver_query.awaitTermination()


if __name__ == "__main__":
    main()