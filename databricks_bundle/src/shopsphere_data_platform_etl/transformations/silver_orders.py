"""Silver: typed, de-duplicated orders, split into valid and quarantined.

Both tables are built from the same de-duplicated stream and the same rule
set (``core.quality.ORDER_RULES``), so every event ends up in exactly one of
them. The validation job checks that this holds on every run.
"""

from pyspark import pipelines as dp

from shopsphere_data_platform_etl.core.orders import deduplicate_events, flatten_orders
from shopsphere_data_platform_etl.core.quality import ORDER_RULES, invalid_orders
from shopsphere_data_platform_etl.core.tables import table_names

TABLES = table_names(spark.conf.get("shopsphere.catalog"))


def deduplicated_orders():
    bronze_stream = spark.readStream.table(TABLES.bronze_events)
    return deduplicate_events(flatten_orders(bronze_stream))


@dp.table(
    name=TABLES.silver_orders,
    comment="Validated, flattened, de-duplicated order events. One row per event.",
    # The change feed is what lets the CDF merge job read only new rows.
    table_properties={"delta.enableChangeDataFeed": "true"},
)
# Rows failing any rule are dropped from this table, and Lakeflow records how
# many failed each named rule in the pipeline event log.
@dp.expect_all_or_drop(ORDER_RULES)
def orders():
    return deduplicated_orders().drop("raw_json", "dedup_key")


@dp.table(
    name=TABLES.silver_invalid_orders,
    comment="Quarantine: order events that failed a data-quality rule, with the reasons and the original JSON.",
)
def invalid_orders_table():
    return invalid_orders(deduplicated_orders()).drop("dedup_key")
