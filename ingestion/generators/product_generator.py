import random
from configs.config import NUM_PRODUCTS

PRODUCT_CATALOG = {
    "Laptop": {
        "brands": ["Dell", "HP", "Lenovo", "Apple", "Asus"],
        "price_range": (40000, 150000)
    },
    "Phone": {
        "brands": ["Apple", "Samsung", "OnePlus", "Google", "Xiaomi"],
        "price_range": (10000, 100000)
    },
    "Headphones": {
        "brands": ["Sony", "JBL", "Boat", "Bose", "Sennheiser"],
        "price_range": (1000, 25000)
    },
    "Monitor": {
        "brands": ["LG", "Samsung", "Dell", "BenQ"],
        "price_range": (8000, 60000)
    },
    "Keyboard": {
        "brands": ["Logitech", "Corsair", "Razer", "HP"],
        "price_range": (800, 10000)
    }
}

def generate_product(product_number):
    category = random.choice(list(PRODUCT_CATALOG.keys()))
    category_info = PRODUCT_CATALOG[category]
    brand = random.choice(category_info["brands"])
    price = random.randint(category_info["price_range"][0], category_info["price_range"][1])

    product_name = f"{brand}{category}"
    product_id = f"P{product_number:06d}"

    return {
        "product_id": product_id,
        "product_name": product_name,
        "category": category,
        "brand": brand,
        "price": price
    }

def generate_products():
    products=[]

    for i in range(1,NUM_PRODUCTS+1):
        products.append(generate_product(i))

    return products