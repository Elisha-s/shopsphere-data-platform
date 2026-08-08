from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


CATALOG = spark.conf.get("shopsphere.catalog")

BRONZE_ORDERS_TABLE = (
    f"{CATALOG}.bronze.eventhub_order_events"
)

SILVER_ORDERS_TABLE = (
    f"{CATALOG}.silver.orders"
)

INVALID_ORDERS_TABLE = (
    f"{CATALOG}.silver.invalid_orders"
)


ORDER_RULES = {
    "event_id_not_null": "event_id IS NOT NULL",
    "order_id_not_null": "order_id IS NOT NULL",
    "customer_id_not_null": "customer_id IS NOT NULL",
    "product_id_not_null": "product_id IS NOT NULL",
    "event_timestamp_not_null": (
        "event_timestamp IS NOT NULL"
    ),
    "positive_quantity": "quantity > 0",
    "positive_unit_price": "unit_price > 0",
    "positive_total_amount": "total_amount > 0",
}


VALID_ORDER_CONDITION = " AND ".join(
    f"({condition})"
    for condition in ORDER_RULES.values()
)


def flatten_orders() -> DataFrame:
    """
    Flatten Bronze event envelopes into typed order records.
    """

    return (
        spark.readStream
        .table(BRONZE_ORDERS_TABLE)
        .select(
            "event_id",
            "event_type",
            "event_version",
            F.to_timestamp(
                "event_timestamp"
            ).alias("event_timestamp"),
            F.to_timestamp(
                "generated_at"
            ).alias("generated_at"),
            "is_late_event",
            F.col(
                "payload.order_id"
            ).alias("order_id"),
            F.col(
                "payload.customer_id"
            ).alias("customer_id"),
            F.col(
                "payload.product_id"
            ).alias("product_id"),
            F.col(
                "payload.product_name"
            ).alias("product_name"),
            F.col(
                "payload.category"
            ).alias("category"),
            F.col(
                "payload.brand"
            ).alias("brand"),
            F.col(
                "payload.quantity"
            ).alias("quantity"),
            F.col(
                "payload.unit_price"
            ).alias("unit_price"),
            F.col(
                "payload.total_amount"
            ).alias("total_amount"),
            F.col(
                "payload.payment_method"
            ).alias("payment_method"),
            F.col(
                "payload.order_status"
            ).alias("order_status"),
            F.col(
                "payload.city"
            ).alias("city"),
            F.col(
                "payload.state"
            ).alias("state"),
            F.col(
                "payload.country"
            ).alias("country"),
            F.to_timestamp(
                "payload.order_timestamp"
            ).alias("order_timestamp"),
            "message_key",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "eventhub_enqueued_timestamp",
            "ingestion_timestamp",
        )
        .withWatermark(
            "event_timestamp",
            "1 day",
        )
        .dropDuplicates(["event_id"])
    )


@dp.table(
    name=SILVER_ORDERS_TABLE,
    comment=(
        "Validated, flattened, and deduplicated "
        "ShopSphere orders."
    ),
    table_properties={ "delta.enableChangeDataFeed": "true" }
)
@dp.expect_all_or_drop(ORDER_RULES)
def silver_orders():
    return flatten_orders()


@dp.table(
    name=INVALID_ORDERS_TABLE,
    comment=(
        "ShopSphere orders that failed Silver "
        "data-quality rules."
    ),
)
def invalid_orders():
    return (
        flatten_orders()
        .filter(
            ~F.expr(VALID_ORDER_CONDITION)
        )
        .withColumn(
            "validation_errors",
            F.concat_ws(
                ",",
                F.when(
                    F.col("event_id").isNull(),
                    F.lit("missing_event_id"),
                ),
                F.when(
                    F.col("order_id").isNull(),
                    F.lit("missing_order_id"),
                ),
                F.when(
                    F.col("customer_id").isNull(),
                    F.lit("missing_customer_id"),
                ),
                F.when(
                    F.col("product_id").isNull(),
                    F.lit("missing_product_id"),
                ),
                F.when(
                    F.col("event_timestamp").isNull(),
                    F.lit("invalid_event_timestamp"),
                ),
                F.when(
                    F.col("quantity") <= 0,
                    F.lit("invalid_quantity"),
                ),
                F.when(
                    F.col("unit_price") <= 0,
                    F.lit("invalid_unit_price"),
                ),
                F.when(
                    F.col("total_amount") <= 0,
                    F.lit("invalid_total_amount"),
                ),
            ),
        )
    )





# from pyspark import pipelines as dp
# from pyspark.sql import functions as F


# @dp.table(
#     name="orders",
#     comment="Validated and flattened ShopSphere orders.",
# )
# @dp.expect_all_or_drop(
#     {
#         "valid_event_id": "event_id IS NOT NULL",
#         "valid_order_id": "order_id IS NOT NULL",
#         "valid_customer_id": "customer_id IS NOT NULL",
#         "valid_product_id": "product_id IS NOT NULL",
#         "positive_quantity": "quantity > 0",
#         "positive_unit_price": "unit_price > 0",
#         "positive_total_amount": "total_amount > 0",
#         "valid_event_timestamp": "event_timestamp IS NOT NULL",
#     }
# )
# def orders():
#     return (
#         spark.readStream.table("order_events")
#         .select(
#             "event_id",
#             "event_type",
#             "event_version",
#             F.to_timestamp("event_timestamp").alias("event_timestamp"),
#             F.to_timestamp("generated_at").alias("generated_at"),
#             "is_late_event",
#             F.col("payload.order_id").alias("order_id"),
#             F.col("payload.customer_id").alias("customer_id"),
#             F.col("payload.product_id").alias("product_id"),
#             F.col("payload.product_name").alias("product_name"),
#             F.col("payload.category").alias("category"),
#             F.col("payload.brand").alias("brand"),
#             F.col("payload.quantity").alias("quantity"),
#             F.col("payload.unit_price").alias("unit_price"),
#             F.col("payload.total_amount").alias("total_amount"),
#             F.col("payload.payment_method").alias("payment_method"),
#             F.col("payload.order_status").alias("order_status"),
#             F.col("payload.city").alias("city"),
#             F.col("payload.state").alias("state"),
#             F.col("payload.country").alias("country"),
#             F.to_timestamp(
#                 "payload.order_timestamp"
#             ).alias("order_timestamp"),
#             "ingestion_timestamp",
#             "source_file",
#         )
#         .dropDuplicates(["event_id"])
#     )