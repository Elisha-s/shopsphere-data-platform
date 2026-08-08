from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    sum,
    count,
    avg
)


def sales_summary(df: DataFrame) -> DataFrame:

    return (
        df.groupBy(
            "country",
            "state",
            "city"
        )
        .agg(
            sum("total_amount").alias("revenue"),

            count("order_id").alias("orders"),

            avg("total_amount").alias("average_order_value")
        )
    )