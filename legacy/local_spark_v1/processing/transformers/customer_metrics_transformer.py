from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    sum,
    count,
    avg,
    max
)


def build_customer_metrics(df: DataFrame) -> DataFrame:

    return (
        df.groupBy("customer_id")
        .agg(
            count("order_id").alias("total_orders"),

            sum("total_amount").alias("lifetime_value"),

            avg("total_amount").alias("average_order_value"),

            max("event_timestamp").alias("last_purchase")
        )
    )