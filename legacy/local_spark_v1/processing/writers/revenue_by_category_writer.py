from configs.config import (
    GOLD_REVENUE_BY_CATEGORY_PATH,
    GOLD_REVENUE_BY_CATEGORY_CHECKPOINT
)


def write_revenue_by_category(df):

    return (
        df.writeStream
        .outputMode("update")
        .format("delta")
        .option(
            "checkpointLocation",
            GOLD_REVENUE_BY_CATEGORY_CHECKPOINT
        )
        .start(GOLD_REVENUE_BY_CATEGORY_PATH)
    )


