from pyspark.sql.functions import col

from configs.config import SILVER_PATH, SILVER_CHECKPOINT 

def write_silver(df):
     
    """
    Write the Silver layer to Delta Lake.
    """

    return (

        df.writeStream

        .format("delta")

        .outputMode("append")

        .option(
            "checkpointLocation",
            SILVER_CHECKPOINT
        )

        .start(
            SILVER_PATH
        )

    )