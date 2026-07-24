import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:9094",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="shopsphere-consumer-group",
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

def consume_events(consumer_name):
    """
    Continuously consume events from the Kafka 'orders' topic.
    """

    print("Waiting for events...\n")

    for message in consumer:

        event = message.value

        print("=" * 50)
        print(f"[{consumer_name}]")
        print(f"Offset      : {message.offset}")
        print(f"Partition   : {message.partition}")
        print(f"Order ID    : {event['payload']['order_id']}")
        print(f"Customer ID : {event['payload']['customer_id']}")
        print(f"Product ID  : {event['payload']['product_id']}")
        print(f"Amount      : ₹{event['payload']['total_amount']:.2f}")
        print("=" * 50)
        consumer.commit()  # Commit the offset after processing the message