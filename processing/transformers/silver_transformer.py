from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def clean_orders(df: DataFrame) -> DataFrame:
    """
    Apply business validation rules to create
    the Silver layer.
    """

    return (
        df

        # Remove invalid order amounts
        .filter(
            col("amount") > 0
        )

        # Mandatory business keys
        .filter(
            col("order_id").isNotNull()
        )

        .filter(
            col("customer_id").isNotNull()
        )

        .filter(
            col("product_id").isNotNull()
        )

        # Remove duplicate orders
        .dropDuplicates(
            ["order_id"]
        )
    )