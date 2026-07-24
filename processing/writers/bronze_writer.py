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
        .start(BRONZE_PATH)
    )