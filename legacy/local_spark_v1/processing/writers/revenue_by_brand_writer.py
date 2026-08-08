from configs.config import (
    GOLD_REVENUE_BY_BRAND_PATH,
    GOLD_REVENUE_BY_BRAND_CHECKPOINT
)


def write_revenue_by_brand(df):

    return (
        df.writeStream
        .outputMode("complete")
        .format("delta")
        .option(
            "checkpointLocation",
            GOLD_REVENUE_BY_BRAND_CHECKPOINT
        )
        .start(GOLD_REVENUE_BY_BRAND_PATH)
    )