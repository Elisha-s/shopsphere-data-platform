"""Customer and product catalogues (the "dimensions" orders refer to).

Both are generated from a fixed seed so they are identical in every run and
on every machine. Without that, product P000001 could be a Dell laptop in one
run and Sony headphones in the next, and any metric grouped by product would
be meaningless across runs.
"""

from __future__ import annotations

import random

from ingestion.config import CATALOG_SEED, COUNTRY, NUM_CUSTOMERS, NUM_PRODUCTS

CITIES = [
    ("Bengaluru", "Karnataka"),
    ("Mumbai", "Maharashtra"),
    ("Delhi", "Delhi"),
    ("Hyderabad", "Telangana"),
    ("Chennai", "Tamil Nadu"),
    ("Pune", "Maharashtra"),
    ("Kolkata", "West Bengal"),
    ("Ahmedabad", "Gujarat"),
    ("Jaipur", "Rajasthan"),
    ("Lucknow", "Uttar Pradesh"),
]

PRODUCT_CATALOG = {
    "Laptop": {"brands": ["Dell", "HP", "Lenovo", "Apple", "Asus"], "price_range": (40000, 150000)},
    "Phone": {"brands": ["Apple", "Samsung", "OnePlus", "Google", "Xiaomi"], "price_range": (10000, 100000)},
    "Headphones": {"brands": ["Sony", "JBL", "Boat", "Bose", "Sennheiser"], "price_range": (1000, 25000)},
    "Monitor": {"brands": ["LG", "Samsung", "Dell", "BenQ"], "price_range": (8000, 60000)},
    "Keyboard": {"brands": ["Logitech", "Corsair", "Razer", "HP"], "price_range": (800, 10000)},
}


def generate_customers(count: int = NUM_CUSTOMERS, seed: int = CATALOG_SEED) -> list[dict]:
    rng = random.Random(seed)
    customers = []
    for number in range(1, count + 1):
        city, state = rng.choice(CITIES)
        customers.append(
            {
                "customer_id": f"C{number:06d}",
                "city": city,
                "state": state,
                "country": COUNTRY,
            }
        )
    return customers


def generate_products(count: int = NUM_PRODUCTS, seed: int = CATALOG_SEED) -> list[dict]:
    # A different stream from the customers so changing one count never
    # reshuffles the other catalogue.
    rng = random.Random(seed + 1)
    categories = list(PRODUCT_CATALOG)
    products = []
    for number in range(1, count + 1):
        category = rng.choice(categories)
        info = PRODUCT_CATALOG[category]
        brand = rng.choice(info["brands"])
        low, high = info["price_range"]
        products.append(
            {
                "product_id": f"P{number:06d}",
                "product_name": f"{brand} {category}",
                "category": category,
                "brand": brand,
                "price": float(rng.randint(low, high)),
            }
        )
    return products
