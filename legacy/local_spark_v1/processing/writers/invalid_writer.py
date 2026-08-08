from configs.config import INVALID_PATH
from configs.config import INVALID_CHECKPOINT


def write_invalid(df):

    return (

        df.writeStream

        .format("delta")

        .outputMode("append")

        .option(
            "checkpointLocation",
            INVALID_CHECKPOINT
        )

        .start(
            INVALID_PATH
        )

    )