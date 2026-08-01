from processing.spark_session import create_spark_session

from processing.readers.kafka_reader import create_kafka_stream
from processing.transformers.parser import parse_orders

from processing.writers.bronze_writer import write_bronze

from processing.quality.validator import split_valid_invalid
from processing.writers.invalid_writer import write_invalid

from processing.logger import get_logger

import time

logger = get_logger(__name__)


def main():

    logger.info("Starting Spark session...")
    spark = create_spark_session()

    logger.info("Creating Kafka stream...")
    raw_stream = create_kafka_stream(spark)

    logger.info("Parsing Kafka events...")
    parsed_stream = parse_orders(raw_stream)

    valid_df, invalid_df = split_valid_invalid(parsed_stream)

    print("===== AFTER VALIDATOR =====")
    valid_df.printSchema()
    print(valid_df.columns)

    logger.info("Starting Bronze stream...")
    bronze_query = write_bronze(valid_df)

    invalid_query = write_invalid(invalid_df)



    logger.info("Streaming pipeline started successfully.")

    

    while True:
        print("\n===== QUERY STATUS =====")

        print("Bronze:")
        print(bronze_query.status)
        print("Exception:", bronze_query.exception())

        print("\nInvalid:")
        print(invalid_query.status)
        print("Exception:", invalid_query.exception())

        time.sleep(5)


if __name__ == "__main__":
    main()