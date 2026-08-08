import time

from ingestion.generators.event_generator import generate_event
from ingestion.producers.kafka_producer import send_event


def main():

    order_number = 1

    while True:

        event = generate_event(order_number)

        send_event(event)

        print(f"Sent {event['payload']['order_id']}")
        print(f"Customer : {event['payload']['customer_id']}")
        print(f"Product  : {event['payload']['product_id']}")
        print(f"Amount   : ₹{event['payload']['total_amount']:.2f}")
        print("-" * 30)

        order_number += 1

        time.sleep(1)


if __name__ == "__main__":
    main()