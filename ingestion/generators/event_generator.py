import uuid
from datetime import datetime, timezone
from ingestion.generators.order_generator import generate_order

def generate_event(order_number):
    generated_at = datetime.now(timezone.utc)
    order = generate_order(order_number)
    is_late_event = order.pop("is_late_event", False)

    order_timestamp = order.get("order_timestamp")

    # JSON cannot serialize Python datetime objects,
    # so convert the timestamp to an ISO-8601 string.
    if isinstance(order_timestamp, datetime):
        order_timestamp = order_timestamp.isoformat()

    order["order_timestamp"] = order_timestamp

    return{
        "event_id": str(uuid.uuid4()),
        "event_type": "ORDER_CREATED",
        "event_version": "1.0",
        "event_timestamp": order['order_timestamp'],
        "payload": order,
        "generated_at": generated_at.isoformat(),
        "is_late_event": is_late_event
    }