import uuid
from ingestion.generators.order_generator import generate_order

def generate_event(order_number):
    order = generate_order(order_number)

    return{
        "event_id": str(uuid.uuid4()),
        "event_type": "ORDER_CREATED",
        "event_version": "1.0",
        "event_timestamp": order['order_timestamp'],
        "payload": order
    }