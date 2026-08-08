from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    sum,
    count
)


def build_product_metrics(df: DataFrame) -> DataFrame:

    return (
        df.groupBy(
            "product_id",
            "product_name"
        )
        .agg(
            sum("quantity").alias("units_sold"),

            sum("total_amount").alias("revenue"),

            count("order_id").alias("orders")
        )
    )