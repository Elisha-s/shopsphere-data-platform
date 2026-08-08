from __future__ import annotations

import json
import os
from typing import Any

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.producer.future import FutureRecordMetadata

load_dotenv()


def _require_environment_variable(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(
            f"Required environment variable '{name}' is missing."
        )

    return value


def _create_event_hubs_producer() -> tuple[KafkaProducer, str]:
    namespace = _require_environment_variable(
        "EVENTHUB_NAMESPACE"
    )

    connection_string = _require_environment_variable(
        "EVENTHUB_PRODUCER_CONNECTION_STRING"
    )

    topic = os.getenv(
        "EVENTHUB_TOPIC",
        "orders",
    )

    bootstrap_servers = (
        f"{namespace}.servicebus.windows.net:9093"
    )

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username="$ConnectionString",
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

    future = producer.send(
        topic,
        value=event,
        key=customer_id,
    )

    metadata = future.get(timeout=30)

    print(
        f"[Producer:eventhubs] "
        f"Customer={customer_id} | "
        f"Topic={metadata.topic} | "
        f"Partition={metadata.partition} | "
        f"Offset={metadata.offset}"
    )

    return metadata