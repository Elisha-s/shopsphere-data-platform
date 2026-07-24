from pyspark.sql.functions import col


def clean_orders(df):

    cleaned = (

        df

        .filter(
            col("amount") > 0
        )

        .filter(
            col("order_id").isNotNull()
        )

        .filter(
            col("customer_id").isNotNull()
        )

        .dropDuplicates(
            ["order_id"]
        )

    )

    return cleaned


def write_silver(df):

    return (

        df.writeStream

        .format("delta")

        .outputMode("append")

        .option(
            "checkpointLocation",
            "data/checkpoints/silver/orders"
        )

        .start(
            "data/silver/orders"
        )

    )