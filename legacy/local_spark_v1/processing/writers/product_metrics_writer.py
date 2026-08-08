from configs.config import (
    GOLD_PRODUCT_METRICS_PATH,
    GOLD_PRODUCT_METRICS_CHECKPOINT
)


def write_product_metrics(df):

    return (
        df.writeStream
        .outputMode("complete")
        .format("delta")
        .option(
            "checkpointLocation",
            GOLD_PRODUCT_METRICS_CHECKPOINT
        )
        .start(GOLD_PRODUCT_METRICS_PATH)
    )