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


CATALOG = spark.conf.get("shopsphere.catalog")

EVENTHUB_NAMESPACE = spark.conf.get(
    "shopsphere.eventhubs.namespace"
)
EVENTHUB_TOPIC = spark.conf.get(
    "shopsphere.eventhubs.topic"
)
CONSUMER_GROUP = spark.conf.get(
    "shopsphere.eventhubs.consumer_group"
)
ACCESS_KEY_NAME = spark.conf.get(
    "shopsphere.eventhubs.access_key_name"
)

SECRET_SCOPE = spark.conf.get(
    "shopsphere.eventhubs.secret_scope"
)

SECRET_KEY = spark.conf.get(
    "shopsphere.eventhubs.secret_key"
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


def escape_jaas_value(value: str) -> str:
    """Escape characters that could break the JAAS configuration."""

    return (
        value
        .replace("\\", "\\\\")
        .replace('"', '\\"')
    )


SHARED_ACCESS_KEY = dbutils.secrets.get(
    scope=SECRET_SCOPE,
    key=SECRET_KEY,
)

EVENTHUB_CONNECTION_STRING = (
    f"Endpoint=sb://{EVENTHUB_NAMESPACE}"
    ".servicebus.windows.net/;"
    f"SharedAccessKeyName={ACCESS_KEY_NAME};"
    f"SharedAccessKey={SHARED_ACCESS_KEY}"
)

SAFE_CONNECTION_STRING = escape_jaas_value(
    EVENTHUB_CONNECTION_STRING
)

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": (
        f"{EVENTHUB_NAMESPACE}.servicebus.windows.net:9093"
    ),
    "subscribe": EVENTHUB_TOPIC,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        "kafkashaded.org.apache.kafka.common.security."
        "plain.PlainLoginModule required "
        'username="$ConnectionString" '
        f'password="{SAFE_CONNECTION_STRING}";'
    ),
    "kafka.group.id": CONSUMER_GROUP,
    "startingOffsets": "earliest",
    "maxOffsetsPerTrigger": "1000",
    "failOnDataLoss": "false",
}


@dp.table(
    name=f"{CATALOG}.bronze.eventhub_order_events",
    comment=(
        "Raw ShopSphere order events ingested from Azure "
        "Event Hubs through its Kafka-compatible endpoint."
    ),
)
def eventhub_order_events():
    kafka_df = (
        spark.readStream
        .format("kafka")
        .options(**KAFKA_OPTIONS)
        .load()
    )

    return (
        kafka_df
        .select(
            F.col("key").cast("string").alias("message_key"),
            F.col("value").cast("string").alias("raw_json"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias(
                "eventhub_enqueued_timestamp"
            ),
        )
        .withColumn(
            "parsed_event",
            F.from_json(
                F.col("raw_json"),
                ORDER_EVENT_SCHEMA,
            ),
        )
        .select(
            "message_key",
            "raw_json",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "eventhub_enqueued_timestamp",
            "parsed_event.*",
        )
        .withColumn(
            "ingestion_timestamp",
            F.current_timestamp(),
        )
    )