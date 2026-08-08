from pyspark.sql import SparkSession, DataFrame
from configs.config import SILVER_PATH

def read_changes(
        spark : SparkSession,
        last_run : str| None
) -> DataFrame :
    
    reader = (spark.read
              .format("delta")
              .option("readChangeFeed","true")
            )
    
    if last_run :
        reader = reader.option(
            "startingTimestamp", last_run
        )

    else :
        reader = reader.option(
            "startingVersion",0
        )

    return reader.load(SILVER_PATH)