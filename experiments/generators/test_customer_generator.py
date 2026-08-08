from ingestion.generators.customer_generator import generate_customers

customers = generate_customers()

print(customers[:5])