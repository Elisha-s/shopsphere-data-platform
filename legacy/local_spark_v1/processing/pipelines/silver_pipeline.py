from processing.spark_session import create_spark_session

from processing.transformers.silver_transformer import clean_orders
from processing.writers.silver_writer import write_silver
from processing.logger import get_logger
from processing.readers.bronze_reader import read_bronze_stream
import time

logger = get_logger(__name__)


def main():

    spark = create_spark_session()

    logger.info("Reading Bronze as stream...")

    bronze_stream = read_bronze_stream(spark)

    logger.info("Applying Silver transformations...")

    silver_df = clean_orders(bronze_stream)

    logger.info("Writing Silver...")

    silver_query = write_silver(silver_df)

    silver_query.awaitTermination()


if __name__ == "__main__":
    main()