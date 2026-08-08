from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    BooleanType
)

ORDER_EVENT_SCHEMA = StructType(
    [
        StructField("event_type", StringType()),

        StructField(
            "payload",
            StructType(
                [
                    StructField("order_id", StringType()),
                    StructField("customer_id", StringType()),
                    StructField("product_id", StringType()),
                    StructField("product_name", StringType()),
                    StructField("category", StringType()),
                    StructField("brand", StringType()),
                    StructField("quantity", IntegerType()),
                    StructField("unit_price", DoubleType()),
                    StructField("total_amount", DoubleType()),
                    StructField("payment_method", StringType()),
                    StructField("order_status", StringType()),
                    StructField("city", StringType()),
                    StructField("state", StringType()),
                    StructField("country", StringType()),
                    StructField("order_timestamp", StringType()),
                ]
            ),
        ),

        StructField("event_timestamp", TimestampType()),
    ]
)