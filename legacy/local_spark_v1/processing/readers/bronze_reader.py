from pyspark.sql import SparkSession

from configs.config import BRONZE_PATH


def read_bronze_stream(spark: SparkSession):
    return (
        spark.readStream
        .format("delta")
        .load(BRONZE_PATH)
    )