"""Change-data-capture logic for the ``current_orders`` table.

``silver.orders`` holds one row per *event*; an order appears once for every
status it has been through. ``silver.current_orders`` holds one row per
*order*: its latest known state.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Change types from Delta Change Data Feed that carry a row's new state.
# "update_preimage" (the old values) and "delete" are not needed here.
NEW_STATE_CHANGE_TYPES = ("insert", "update_postimage")

CDF_METADATA_COLUMNS = ("_change_type", "_commit_version", "_commit_timestamp")

MERGE_KEY_CONDITION = "target.order_id = source.order_id"

# Only overwrite an order with an event that is newer than what is stored.
# Without this, an event that arrives late (after a newer one was already
# merged) would roll the order back to an older status. The event_id
# comparison is a tie-break so the result does not depend on arrival order
# when two events carry the same timestamp.
SOURCE_IS_NEWER_CONDITION = (
    "source.event_timestamp > target.event_timestamp "
    "OR (source.event_timestamp = target.event_timestamp AND source.event_id > target.event_id)"
)


def latest_change_per_order(changes: DataFrame) -> DataFrame:
    """Reduce a batch of change rows to one row per order: its newest event.

    "Newest" means latest *event time* (when it happened), not latest commit
    (when it was written): a late-arriving event is written last but is not
    the newest state. MERGE also requires at most one source row per key.
    """
    newest_first = Window.partitionBy("order_id").orderBy(
        F.col("event_timestamp").desc(),
        F.col("event_id").desc(),
    )
    return (
        changes.filter(F.col("_change_type").isin(*NEW_STATE_CHANGE_TYPES))
        .withColumn("_row_number", F.row_number().over(newest_first))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number", *CDF_METADATA_COLUMNS)
    )
