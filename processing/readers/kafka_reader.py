from pyspark.sql import SparkSession, DataFrame

from configs.config import (KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, STARTING_OFFSETS)

def create_kafka_stream(spark:SparkSession) -> DataFrame:
    """
    Create a streaming DataFrame from Kafka.

    Args:
        spark:
            Active SparkSession.

    Returns:
        Streaming DataFrame containing Kafka messages.
    """

    return(
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .load()
    )