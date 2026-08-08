from configs.config import (
    GOLD_CUSTOMER_METRICS_PATH,
    GOLD_CUSTOMER_METRICS_CHECKPOINT
)


def write_customer_metrics(df):

    return (
        df.writeStream
        .outputMode("complete")
        .format("delta")
        .option(
            "checkpointLocation",
            GOLD_CUSTOMER_METRICS_CHECKPOINT
        )
        .start(GOLD_CUSTOMER_METRICS_PATH)
    )