from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)
from pyspark.sql import DataFrame


# ==========================================================
# Event Schema
# ==========================================================
order_event_schema = StructType([
    StructField("event_type", StringType()),

    StructField(
        "payload",

        StructType([

            StructField("order_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("product_id", StringType()),
            StructField("total_amount", DoubleType())

        ])
    ),

    StructField("event_timestamp", TimestampType())
])


def parse_orders(df: DataFrame) -> DataFrame:
    """
    Parse Kafka order events from JSON into a flattened Spark DataFrame.
    """
    parsed = (
        df
        .selectExpr("CAST(value AS STRING)")
        .select(
            from_json(
                col("value"),
                order_event_schema
            ).alias("data")
        )
    )

    return (
        parsed.select(
            col("data.event_type"),
            col("data.payload.order_id"),
            col("data.payload.customer_id"),
            col("data.payload.product_id"),
            col("data.payload.total_amount").alias("amount"),
            col("data.event_timestamp")
        )
    )