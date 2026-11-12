"""Producer for Azure Event Hubs.

Event Hubs exposes a Kafka-compatible endpoint, so this is the ordinary Kafka
producer with Event Hubs connection settings: TLS on port 9093 and SASL PLAIN
with the literal username ``$ConnectionString``.

Required environment variables (see ``.env.example``):
    EVENTHUB_NAMESPACE                     e.g. shopsphere-events
    EVENTHUB_PRODUCER_CONNECTION_STRING    a connection string with Send rights
Optional:
    EVENTHUB_TOPIC                         defaults to "orders"
"""

from __future__ import annotations

import os

from dotenv import load_dotenv

from ingestion.producers.kafka_producer import OrderEventProducer


def _require_environment_variable(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable '{name}' is missing.")
    return value


def create_event_hubs_producer() -> OrderEventProducer:
    """Build the producer. Nothing connects until this function is called."""
    load_dotenv()
    namespace = _require_environment_variable("EVENTHUB_NAMESPACE")
    connection_string = _require_environment_variable("EVENTHUB_PRODUCER_CONNECTION_STRING")
    topic = os.getenv("EVENTHUB_TOPIC", "orders")

    return OrderEventProducer(
        topic=topic,
        bootstrap_servers=f"{namespace}.servicebus.windows.net:9093",
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username="$ConnectionString",
        sasl_plain_password=connection_string,
        request_timeout_ms=30_000,
    )
