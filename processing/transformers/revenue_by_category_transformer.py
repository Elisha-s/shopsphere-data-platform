from pyspark.sql import DataFrame
from pyspark.sql.functions import sum, count, col, window


def revenue_by_category(df: DataFrame) -> DataFrame:

    return (
        df
        .withWatermark("event_timestamp","5 minutes")
        .groupBy(
            window(col("event_timestamp"), "1 minute"),
            col("category")
        .agg(
            sum("total_amount").alias("total_revenue"),
            count("order_id").alias("total_orders")
        )
     )
    )