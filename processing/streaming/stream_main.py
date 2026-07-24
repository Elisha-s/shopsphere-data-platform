from processing.spark_session import create_spark_session

from processing.readers.kafka_reader import create_kafka_stream
from processing.transformers.parser import parse_orders

from processing.writers.bronze_writer import write_bronze
from processing.writers.silver_writer import write_silver
from processing.transformers.silver_transformer import clean_orders

from processing.logger import get_logger

logger = get_logger(__name__)


def main():

    logger.info("Starting Spark session...")
    spark = create_spark_session()

    logger.info("Creating Kafka stream...")
    raw_stream = create_kafka_stream(spark)

    logger.info("Parsing Kafka events...")
    parsed_stream = parse_orders(raw_stream)

    logger.info("Starting Bronze stream...")
    bronze_query = write_bronze(parsed_stream)

    logger.info("Applying Silver transformations...")
    silver_df = clean_orders(parsed_stream)

    logger.info("Starting Silver stream...")
    silver_query = write_silver(silver_df)

    logger.info("Streaming pipeline started successfully.")
    
    bronze_query.awaitTermination()
    silver_query.awaitTermination()


if __name__ == "__main__":
    main()