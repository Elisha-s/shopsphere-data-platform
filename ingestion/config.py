"""Settings for the synthetic order generator.

Everything here can be overridden from the command line of
``python -m ingestion.run_producer``; these are only the defaults.
"""

NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 500
COUNTRY = "India"

# The customer and product catalogues are built from this fixed seed, so
# customer C000042 and product P000007 are the same customer and product in
# every run. Orders are random; the dimensions they refer to are not.
CATALOG_SEED = 20260101

# Share of events that are deliberately "wrong", so the pipeline's data-quality,
# deduplication and late-data handling have something to work on.
INVALID_RATE = 0.02  # broken records that must land in silver.invalid_orders
DUPLICATE_RATE = 0.02  # the same event delivered twice (same event_id)
LATE_RATE = 0.05  # events delivered LATE_DELAY after they happened
LATE_DELAY_MINUTES = 7

# Chance that a tick advances an existing order instead of creating a new one.
UPDATE_SHARE = 0.45
# Chance that an order which is still CREATED gets cancelled instead of paid.
CANCEL_SHARE = 0.08

KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"
KAFKA_TOPIC = "orders"
LANDING_DIR = "data/landing/orders"
