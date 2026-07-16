from configs.config import NUM_CUSTOMERS,COUNTRY
import random

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
    ("Lucknow", "Uttar Pradesh")
]


def generate_customer(customer_number):
    city, state = random.choice(CITIES)
    customer_id = f"C{customer_number:06d}"

    return {
    "customer_id": customer_id,
    "city": city,
    "state": state,
    "country": COUNTRY
   }


def generate_customers():
    customers = []

    for i in range(1, NUM_CUSTOMERS + 1):
        customers.append(generate_customer(i))

    return customers    