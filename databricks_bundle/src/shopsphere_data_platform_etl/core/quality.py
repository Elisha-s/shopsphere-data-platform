"""Data-quality rules for Silver orders.

One dictionary, ``ORDER_RULES``, is the single definition of "a valid order".
It is used three ways, which is what keeps them consistent:

* Lakeflow expectations on the Silver table (``expect_all_or_drop``),
* the filter that routes failing rows to the quarantine table,
* the ``validation_errors`` column that names every rule a row broke.

Every rule is written so that it evaluates to TRUE or FALSE, never NULL.
In SQL, ``quantity > 0`` is NULL when quantity is NULL, and a NULL condition
is neither kept by a filter nor by its negation: such a row would silently
disappear from both the valid and the invalid table. ``coalesce(..., false)``
closes that gap.
"""

from __future__ import annotations

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

VALID_ORDER_STATUSES = ("CREATED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED")

_STATUS_LIST = ", ".join(f"'{status}'" for status in VALID_ORDER_STATUSES)

ORDER_RULES: dict[str, str] = {
    "event_id_present": "event_id IS NOT NULL",
    "order_id_present": "order_id IS NOT NULL",
    "customer_id_present": "customer_id IS NOT NULL",
    "product_id_present": "product_id IS NOT NULL",
    "event_timestamp_valid": "event_timestamp IS NOT NULL",
    "quantity_positive": "coalesce(quantity > 0, false)",
    "unit_price_positive": "coalesce(unit_price > 0, false)",
    "total_amount_positive": "coalesce(total_amount > 0, false)",
    "total_amount_consistent": "coalesce(abs(total_amount - quantity * unit_price) < 0.01, false)",
    "order_status_known": f"coalesce(order_status IN ({_STATUS_LIST}), false)",
}


def is_valid_order() -> Column:
    """TRUE when a row passes every rule."""
    return reduce(lambda left, right: left & right, [F.expr(rule) for rule in ORDER_RULES.values()])


def validation_errors() -> Column:
    """Comma-separated names of the rules a row breaks ('' when it breaks none)."""
    return F.concat_ws(
        ",",
        *[F.when(~F.expr(rule), F.lit(name)) for name, rule in ORDER_RULES.items()],
    )


def valid_orders(orders: DataFrame) -> DataFrame:
    return orders.filter(is_valid_order())


def invalid_orders(orders: DataFrame) -> DataFrame:
    """Rows that break at least one rule, with the reasons attached."""
    return orders.filter(~is_valid_order()).withColumn("validation_errors", validation_errors())
