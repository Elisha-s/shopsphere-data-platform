"""Shared Kafka-protocol producer.

Used directly for the local Kafka broker, and by ``eventhub_producer`` for
Azure Event Hubs (which speaks the Kafka protocol, so only the connection
settings differ).
"""

from __future__ import annotations

import json
import logging
from typing import Any

from kafka import KafkaProducer

from ingestion import config

logger = logging.getLogger(__name__)


class OrderEventProducer:
    """Sends order events to one topic, keyed by order id.

    Keying by ``order_id`` puts every event of an order on the same partition,
    and a partition preserves order. That keeps one order's status changes in
    sequence for any consumer.
    """

    def __init__(self, topic: str = config.KAFKA_TOPIC, **connection_settings: Any):
        self.topic = topic
        self.failed = 0
        settings = {"bootstrap_servers": config.KAFKA_BOOTSTRAP_SERVERS}
        settings.update(connection_settings)
        self._producer = KafkaProducer(
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            key_serializer=lambda key: str(key).encode("utf-8"),
            acks="all",  # wait for the broker to confirm the write
            retries=5,
            linger_ms=50,  # let the client batch messages for up to 50 ms
            **settings,
        )

    def send(self, event: dict[str, Any]) -> None:
        """Queue one event. Delivery happens in the background, in batches."""
        # An invalid test event may have no order_id; fall back to the event id.
        key = event["payload"].get("order_id") or event["event_id"]
        future = self._producer.send(self.topic, value=event, key=key)
        future.add_errback(self._on_error)

    def _on_error(self, error: Exception) -> None:
        self.failed += 1
        logger.error("Failed to deliver an event to %s: %s", self.topic, error)

    def close(self) -> None:
        """Block until everything queued has been delivered, then disconnect."""
        self._producer.flush()
        self._producer.close()
