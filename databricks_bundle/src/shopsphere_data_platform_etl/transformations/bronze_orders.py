"""Bronze: raw order events from Azure Event Hubs (Lakeflow streaming table).

Event Hubs is read through its Kafka-compatible endpoint with Spark's Kafka
source. The shared access key comes from a Databricks secret scope and is
never written in code or configuration. Parsing lives in the shared ``core``
package so the local pipeline produces an identical Bronze table.

``spark`` and ``dbutils`` are provided by the Lakeflow runtime.
"""

from pyspark import pipelines as dp

from shopsphere_data_platform_etl.core.orders import kafka_to_transport, parse_events
from shopsphere_data_platform_etl.core.tables import table_names

TABLES = table_names(spark.conf.get("shopsphere.catalog"))

EVENTHUB_NAMESPACE = spark.conf.get("shopsphere.eventhubs.namespace")
# Through the Kafka interface, an Event Hub is addressed as a "topic".
EVENTHUB_TOPIC = spark.conf.get("shopsphere.eventhubs.topic")
CONSUMER_GROUP = spark.conf.get("shopsphere.eventhubs.consumer_group")
ACCESS_KEY_NAME = spark.conf.get("shopsphere.eventhubs.access_key_name")
SECRET_SCOPE = spark.conf.get("shopsphere.eventhubs.secret_scope")
SECRET_KEY = spark.conf.get("shopsphere.eventhubs.secret_key")


def escape_jaas_value(value: str) -> str:
    """Escape characters that would break the quoted JAAS password string."""
    return value.replace("\\", "\\\\").replace('"', '\\"')


SHARED_ACCESS_KEY = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)

EVENTHUB_CONNECTION_STRING = (
    f"Endpoint=sb://{EVENTHUB_NAMESPACE}.servicebus.windows.net/;"
    f"SharedAccessKeyName={ACCESS_KEY_NAME};"
    f"SharedAccessKey={SHARED_ACCESS_KEY}"
)

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": f"{EVENTHUB_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": EVENTHUB_TOPIC,
    "kafka.security.protocol": "SASL_SSL",  # authenticated and encrypted
    "kafka.sasl.mechanism": "PLAIN",
    # Event Hubs expects the literal username "$ConnectionString" and the
    # connection string as the password.
    "kafka.sasl.jaas.config": (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        'username="$ConnectionString" '
        f'password="{escape_jaas_value(EVENTHUB_CONNECTION_STRING)}";'
    ),
    "kafka.group.id": CONSUMER_GROUP,
    # Only used the very first time the stream starts. After that the
    # pipeline's checkpoint decides where to resume.
    "startingOffsets": "earliest",
    "maxOffsetsPerTrigger": "1000",
    # Trade-off: with "false" the stream keeps running if Event Hubs has
    # already expired messages the checkpoint expected (retention is days,
    # not forever), at the cost of skipping them without failing. Acceptable
    # for a demo that is paused for long periods; a production pipeline
    # would set "true" and alert.
    "failOnDataLoss": "false",
}


@dp.table(
    name=TABLES.bronze_events,
    comment="Raw ShopSphere order events from Azure Event Hubs, with transport metadata and the original JSON.",
)
def order_events():
    kafka_df = spark.readStream.format("kafka").options(**KAFKA_OPTIONS).load()
    return parse_events(kafka_to_transport(kafka_df))
