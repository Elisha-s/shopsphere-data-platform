from pyspark.sql.functions import col, from_json
from pyspark.sql import DataFrame
from processing.schemas.order_schema import ORDER_EVENT_SCHEMA



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
                ORDER_EVENT_SCHEMA
            ).alias("data")
        )
    )

    return parsed.select(

    col("data.event_type"),

    col("data.payload.order_id"),
    col("data.payload.customer_id"),
    col("data.payload.product_id"),
    col("data.payload.product_name"),
    col("data.payload.category"),
    col("data.payload.brand"),
    col("data.payload.quantity"),
    col("data.payload.unit_price"),

    col("data.payload.total_amount"),

    col("data.payload.payment_method"),
    col("data.payload.order_status"),

    col("data.payload.city"),
    col("data.payload.state"),
    col("data.payload.country"),

    col("data.payload.order_timestamp"),

    col("data.event_timestamp")
)