from __future__ import annotations

import json
import os
from typing import Any

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.producer.future import FutureRecordMetadata

load_dotenv()

# If an essential configuration value is absent, fail immediately with a useful error.
def _require_environment_variable(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(
            f"Required environment variable '{name}' is missing."
        )

    return value


# creates and configures the connection to Azure Event Hubs. 
# It returns a tuple containing the KafkaProducer instance and the topic/Event Hub name.
def _create_event_hubs_producer() -> tuple[KafkaProducer, str]:
    namespace = _require_environment_variable(
        "EVENTHUB_NAMESPACE"
    )

    connection_string = _require_environment_variable(
        "EVENTHUB_PRODUCER_CONNECTION_STRING"
    )

    # orders is the default value for the topic, but it can be overridden by setting the EVENTHUB_TOPIC environment variable.
    topic = os.getenv(
        "EVENTHUB_TOPIC",
        "orders",
    )

    # Connect to this Azure Event Hubs namespace using Kafka protocol over TLS.
    bootstrap_servers = (
        f"{namespace}.servicebus.windows.net:9093"
    )


    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers, 
        security_protocol="SASL_SSL", # Authenticate using SASL and encrypt the connection using TLS.
        sasl_mechanism="PLAIN",
        sasl_plain_username="$ConnectionString", #For Azure Event Hubs Kafka authentication, $ConnectionString is a special expected value.
        sasl_plain_password=connection_string,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: str(key).encode("utf-8"),
        acks="all",
        retries=5,
        request_timeout_ms=30_000,
    )

    return producer, topic


producer, topic = _create_event_hubs_producer()


def send_event(
    event: dict[str, Any],
) -> FutureRecordMetadata:

    customer_id = event["payload"]["customer_id"]

    # send() doesn't necessarily wait for the broker response before returning.
    future = producer.send(
        topic,
        value=event,
        key=customer_id,
    )

    # Give Event Hubs up to 30 seconds to acknowledge the send.
    metadata = future.get(timeout=30)

    print(
        f"[Producer:eventhubs] "
        f"Customer={customer_id} | "
        f"Topic={metadata.topic} | "
        f"Partition={metadata.partition} | "
        f"Offset={metadata.offset}"
    )

    return metadata