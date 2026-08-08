from pyspark.sql import DataFrame
from pyspark.sql.functions import sum, count


def revenue_by_brand(df: DataFrame) -> DataFrame:

    return (
        df.groupBy("brand")
        .agg(
            sum("total_amount").alias("total_revenue"),
            count("order_id").alias("total_orders")
        )
    )