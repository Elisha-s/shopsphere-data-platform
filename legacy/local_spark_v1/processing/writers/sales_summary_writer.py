from configs.config import (
    GOLD_SALES_SUMMARY_PATH,
    GOLD_SALES_SUMMARY_CHECKPOINT
)


def write_sales_summary(df):

    return (
        df.writeStream
        .outputMode("complete")
        .format("delta")
        .option(
            "checkpointLocation",
            GOLD_SALES_SUMMARY_CHECKPOINT
        )
        .start(GOLD_SALES_SUMMARY_PATH)
    )