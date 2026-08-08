# from configs.config import SILVER_PATH, SILVER_CHECKPOINT


# def write_silver(df):

#     return (
#         df.writeStream
#         .format("delta")
#         .outputMode("append")
#         .option(
#             "checkpointLocation",
#             SILVER_CHECKPOINT
#         )
#         .option(
#             "delta.enableChangeDataFeed",
#             "true"
#         )
#         .start(SILVER_PATH)
#     )

from configs.config import SILVER_PATH, SILVER_CHECKPOINT


def write_silver(df):

    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            SILVER_CHECKPOINT
        )
        .option(
            "path",
            SILVER_PATH
        )
        .option(
            "mergeSchema",
            "true"
        )
        .trigger(processingTime="5 seconds")
        .toTable(
            "silver_orders",
            tableProperties={
                "delta.enableChangeDataFeed": "true"
            }
        )
    )


