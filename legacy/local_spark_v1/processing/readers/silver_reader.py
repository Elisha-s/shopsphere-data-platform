from pyspark.sql import SparkSession

from configs.config import SILVER_PATH


def read_silver_stream(spark: SparkSession):
    return (
        spark.readStream
        .format("delta")
        .load(SILVER_PATH)
    )