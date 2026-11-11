"""Table names, in one place.

On Databricks the names are three-part Unity Catalog names
(``<catalog>.silver.orders``). Locally there is no catalog, so the names are
two-part (``silver.orders``). Dev and prod are separated by catalog.
"""

from __future__ import annotations

from dataclasses import dataclass

SCHEMAS = ("bronze", "silver", "gold", "metadata")


@dataclass(frozen=True)
class Tables:
    bronze_events: str
    silver_orders: str
    silver_invalid_orders: str
    silver_current_orders: str
    gold_order_activity: str
    gold_customer_metrics: str
    gold_product_metrics: str
    pipeline_state: str
    schemas: tuple[str, ...]


def table_names(catalog: str | None = None) -> Tables:
    prefix = f"{catalog}." if catalog else ""
    return Tables(
        bronze_events=f"{prefix}bronze.order_events",
        silver_orders=f"{prefix}silver.orders",
        silver_invalid_orders=f"{prefix}silver.invalid_orders",
        silver_current_orders=f"{prefix}silver.current_orders",
        gold_order_activity=f"{prefix}gold.order_activity_by_status",
        gold_customer_metrics=f"{prefix}gold.customer_metrics",
        gold_product_metrics=f"{prefix}gold.product_metrics",
        pipeline_state=f"{prefix}metadata.pipeline_state",
        schemas=tuple(f"{prefix}{schema}" for schema in SCHEMAS),
    )
