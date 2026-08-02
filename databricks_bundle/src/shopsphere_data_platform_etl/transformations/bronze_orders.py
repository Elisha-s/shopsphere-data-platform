from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


LANDING_PATH = (
    "/Volumes/"
    "adb_shopsphere_dev/"
    "bronze/"
    "order_events_landing"
)


PAYLOAD_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("order_timestamp", StringType(), True),
    ]
)


ORDER_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_version", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("generated_at", StringType(), True),
        StructField("is_late_event", BooleanType(), True),
        StructField("payload", PAYLOAD_SCHEMA, True),
    ]
)


@dp.table(
    name="order_events",
    comment="Raw ShopSphere order events incrementally ingested from JSON.",
)
def order_events():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(ORDER_EVENT_SCHEMA)
        .load(LANDING_PATH)
        .withColumn(
            "ingestion_timestamp",
            F.current_timestamp(),
        )
        .withColumn(
            "source_file",
            F.col("_metadata.file_path"),
        )
    )