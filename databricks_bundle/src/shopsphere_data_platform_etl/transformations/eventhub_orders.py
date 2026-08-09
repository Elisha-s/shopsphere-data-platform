# eventhub_orders.py defines a Lakeflow-managed Bronze streaming table. It uses Spark Structured Streaming's 
# Kafka connector to consume ShopSphere events from Azure Event Hubs' Kafka-compatible endpoint, authenticates 
# using a secret stored in Databricks, preserves source metadata and raw JSON for traceability, parses the 
# event using an explicit schema, and adds an ingestion timestamp before persisting the stream into Bronze."


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

# This identifies the Event Hub you want to consume. Because we're using the Kafka interface, the code calls it a topic.
EVENTHUB_TOPIC = spark.conf.get(
    "shopsphere.eventhubs.topic"
)
CONSUMER_GROUP = spark.conf.get(
    "shopsphere.eventhubs.consumer_group"
)
# This is the name of the Event Hubs authorization policy/key.
ACCESS_KEY_NAME = spark.conf.get(
    "shopsphere.eventhubs.access_key_name"
)

# These tell Databricks where to find the secret.
SECRET_SCOPE = spark.conf.get(
    "shopsphere.eventhubs.secret_scope"
)

SECRET_KEY = spark.conf.get(
    "shopsphere.eventhubs.secret_key"
)


# All timestamps are StringType because the event generator produces ISO-8601 strings, and we want to preserve the original format. We can convert to TimestampType in silver layer.
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

# This is security/configuration plumbing.
# Later your connection string is embedded inside a JAAS authentication string
def escape_jaas_value(value: str) -> str:
    """Escape characters that could break the JAAS(Java Authentication and Authorization Service) configuration."""

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

# makes it safe to embed inside the Kafka JAAS configuration.
SAFE_CONNECTION_STRING = escape_jaas_value(
    EVENTHUB_CONNECTION_STRING
)

# Spark connects to Event Hubs through its Kafka-compatible endpoint using SASL/SSL authentication, 
# with the shared access credential retrieved from Databricks Secrets.
KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": (
        f"{EVENTHUB_NAMESPACE}.servicebus.windows.net:9093"
    ),
    "subscribe": EVENTHUB_TOPIC,
    "kafka.security.protocol": "SASL_SSL", # authentication + encrypted transport
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        "kafkashaded.org.apache.kafka.common.security."
        "plain.PlainLoginModule required "
        'username="$ConnectionString" '
        f'password="{SAFE_CONNECTION_STRING}";'
    ),
    "kafka.group.id": CONSUMER_GROUP,
    "startingOffsets": "earliest", # If this streaming query starts without existing progress information, begin from the earliest available offsets.
    "maxOffsetsPerTrigger": "1000",
    "failOnDataLoss": "false",
}

# Lakeflow takes the returned kafka_df DataFrame and manages the target Bronze table. 
# @dp.table - Declare a Lakeflow-managed table
@dp.table(
    name=f"{CATALOG}.bronze.eventhub_order_events",
    comment=(
        "Raw ShopSphere order events ingested from Azure Event Hubs through its Kafka-compatible endpoint."
    ),
)
def eventhub_order_events():
    kafka_df = (
        spark.readStream
        .format("kafka") # telling Spark to use its Kafka source connector
        .options(**KAFKA_OPTIONS) # ** expands the Python dictionary.
        .load()
    )

    return (
        kafka_df
        .select(
            F.col("key").cast("string").alias("message_key"), # key and value received as binary data, so we cast to string for easier processing.
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
        # Flatten the parsed_event struct into individual columns, while keeping the payload as a 
        # nested struct for later processing in Silver.
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
            "ingestion_timestamp",   # When did our Databricks ingestion pipeline process this record?
            F.current_timestamp(),
        )
    )