from pyspark.sql.functions import col


def split_valid_invalid(df):

    valid_condition = (
        col("order_id").isNotNull()
        & col("customer_id").isNotNull()
        & col("product_id").isNotNull()
        & (col("total_amount") > 0)
    )

    valid = df.filter(valid_condition)

    invalid = df.filter(~valid_condition)

    return valid, invalid