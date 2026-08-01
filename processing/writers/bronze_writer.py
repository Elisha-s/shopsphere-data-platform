from pyspark.sql import DataFrame
from configs.config import BRONZE_PATH, BRONZE_CHECKPOINT 


def write_bronze(df: DataFrame):

    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            BRONZE_CHECKPOINT
        )
        .trigger(processingTime="5 seconds")
        .start(BRONZE_PATH)
    )