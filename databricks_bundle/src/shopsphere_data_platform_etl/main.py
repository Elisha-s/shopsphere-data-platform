from __future__ import annotations

import argparse
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


CATEGORIES = [
    "Electronics",
    "Fashion",
    "Home",
    "Books",
    "Sports",
]

BRANDS = [
    "Nova",
    "Orion",
    "Zenith",
    "Apex",
    "Vertex",
]

LOCATIONS = [
    ("Bengaluru", "Karnataka", "India"),
    ("Mumbai", "Maharashtra", "India"),
    ("Delhi", "Delhi", "India"),
    ("Lucknow", "Uttar Pradesh", "India"),
]

PAYMENT_METHODS = [
    "UPI",
    "CARD",
    "NET_BANKING",
    "WALLET",
]

ORDER_STATUSES = [
    "CREATED",
    "PAID",
    "SHIPPED",
    "DELIVERED",
]


def generate_order_event(order_number: int) -> dict:
    generated_at = datetime.now(timezone.utc)

    # Approximately 5% of events have an event time
    # seven minutes older than their generation time.
    is_late_event = random.random() < 0.05

    event_timestamp = (
        generated_at - timedelta(minutes=7)
        if is_late_event
        else generated_at
    )

    product_number = random.randint(1, 500)
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(500, 100_000), 2)

    city, state, country = random.choice(LOCATIONS)

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "ORDER_CREATED",
        "event_version": "1.0",
        "event_timestamp": event_timestamp.isoformat(),
        "generated_at": generated_at.isoformat(),
        "is_late_event": is_late_event,
        "payload": {
            "order_id": f"O{order_number:08d}",
            "customer_id": f"C{random.randint(1, 1000):06d}",
            "product_id": f"P{product_number:06d}",
            "product_name": f"Product {product_number}",
            "category": random.choice(CATEGORIES),
            "brand": random.choice(BRANDS),
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": round(quantity * unit_price, 2),
            "payment_method": random.choice(PAYMENT_METHODS),
            "order_status": random.choice(ORDER_STATUSES),
            "city": city,
            "state": state,
            "country": country,
            "order_timestamp": event_timestamp.isoformat(),
        },
    }


def write_event_batch(
    output_path: str,
    number_of_events: int,
) -> Path:
    if number_of_events <= 0:
        raise ValueError("number_of_events must be greater than zero.")

    destination = Path(output_path)
    destination.mkdir(parents=True, exist_ok=True)

    batch_id = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%S%f"
    )

    file_path = destination / f"orders_{batch_id}.json"

    with file_path.open("w", encoding="utf-8") as output_file:
        for order_number in range(1, number_of_events + 1):
            event = generate_order_event(order_number)
            output_file.write(json.dumps(event) + "\n")

    return file_path


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate ShopSphere order events."
    )

    parser.add_argument("--output-path", required=True)

    parser.add_argument(
        "--number-of-events",
        type=int,
        default=500,
    )

    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    output_file = write_event_batch(
        output_path=arguments.output_path,
        number_of_events=arguments.number_of_events,
    )

    print(
        f"Generated {arguments.number_of_events} order events "
        f"in {output_file}"
    )


if __name__ == "__main__":
    main()