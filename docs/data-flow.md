# Data Flow

## 1. Event generation
A Databricks Python wheel task generates 500 order events per run, each with event
metadata, customer/product identifiers, order details, location, payment method, and
timestamps. ~5% of events are assigned an event timestamp 7 minutes earlier than their
generation timestamp to simulate late-arriving data.

## 2. Landing
Events are written as newline-delimited JSON to
`/Volumes/adb_shopsphere_dev/bronze/order_events_landing`, picked up by Auto Loader.

## 3. Bronze
`eventhub_orders.py` consumes from Event Hubs via its Kafka-compatible endpoint
(SASL_SSL, credentials from Databricks Secrets), parses the JSON payload against an
explicit schema, and persists to `bronze.eventhub_order_events` with an ingestion
timestamp and Kafka metadata preserved for lineage.

## 4. Silver
`silver_orders.py` reads Bronze incrementally, flattens the nested payload, applies a
1-day watermark with `event_id` dedup, and enforces data-quality rules
(`expect_all_or_drop`). Passing records land in `silver.orders` (with Change Data Feed
enabled); failing records land in `silver.invalid_orders` with a `validation_errors`
column listing each specific failure.

## 5. CDF merge
`cdf_merge.py` reads only the unprocessed Delta CDF version range from `silver.orders`,
keeps the latest change per `order_id`, and MERGEs into `silver.current_orders`. The
processed-version checkpoint only advances after the MERGE succeeds.

## 6. Gold
`gold_metrics.py` produces a streaming, 1-minute event-time windowed aggregation
(`order_activity_by_status`) directly from `silver.orders`, and two materialized views
(`customer_metrics`, `product_metrics`) computed over `silver.current_orders`.

## 7. Validation
`validate_outputs.py` reconciles record counts across all layers and fails loudly on
mismatch or empty gold tables.

## Orchestration
All of the above is sequenced by the ADF pipeline `PL_SHOPSPHERE_MEDALLION` — see
[`orchestration/adf/README.md`](../orchestration/adf/README.md).