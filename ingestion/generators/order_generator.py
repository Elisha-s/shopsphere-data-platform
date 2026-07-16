import random
from datetime import datetime

from ingestion.generators.customer_generator import generate_customers
from ingestion.generators.product_generator import generate_products

customers = generate_customers()
products = generate_products()


def generate_order(order_number):
    customer = random.choice(customers)
    product = random.choice(products)

    quantity = random.randint(1, 5)

    payment_method = random.choice(
        ["UPI", "CARD", "NET_BANKING", "WALLET"]
    )

    status = random.choice(
        ["CREATED", "PAID", "SHIPPED", "DELIVERED"]
    )

    timestamp = datetime.utcnow().isoformat()

    total_amount = quantity * product["price"]

    return {
        "order_id": f"O{order_number:08d}",
        "customer_id": customer["customer_id"],
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "brand": product["brand"],
        "quantity": quantity,
        "unit_price": product["price"],
        "total_amount": total_amount,
        "payment_method": payment_method,
        "order_status": status,
        "city": customer["city"],
        "state": customer["state"],
        "country": customer["country"],
        "order_timestamp": timestamp
    }