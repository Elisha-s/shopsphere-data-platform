# Data flow

What happens to an order event, step by step. File references are relative to
`databricks_bundle/src/shopsphere_data_platform_etl/`.

## 1. Generation (`ingestion/`)

`EventGenerator` simulates a shop. Each tick it either creates an order
(`ORDER_CREATED`, status `CREATED`) or advances an existing one
(`ORDER_STATUS_CHANGED`: `CREATED → PAID → SHIPPED → DELIVERED`, or
`CREATED → CANCELLED`). Every event carries the full order snapshot.

Customers and products come from catalogues built with a fixed seed, so
`P000007` is the same product in every run. Order ids contain a run id, so two
runs never reuse an id.

A configurable share of events is deliberately problematic:

| Kind | Default share | What it is | Where the pipeline handles it |
|---|---|---|---|
| Invalid | 2% | missing id, negative quantity, missing or inconsistent amount, bad timestamp, unknown status | Silver quality rules → `silver.invalid_orders` |
| Duplicate | 2% | the same event (same `event_id`) delivered twice | Silver de-duplication |
| Late | 5% | delivered 7 minutes after it happened, possibly after a newer event for the same order | CDF merge ordering and guard; Gold watermark |

The generator counts what it injects. The tests compare those counts with the
pipeline's tables.

## 2. Transport

- **Files** (`--target files`): newline-delimited JSON files in a landing folder.
- **Kafka** (`--target kafka`): topic `orders`, keyed by `order_id`.
- **Event Hubs** (`--target eventhubs`): the same Kafka client with Event Hubs
  connection settings.

Keying by `order_id` keeps all events of one order on one partition, in order.

## 3. Bronze: `bronze.order_events`

`core/orders.py::parse_events`. Each message becomes one row holding:

- transport metadata: key, topic, partition, offset, arrival time
  (`enqueued_timestamp`);
- `raw_json`: the message exactly as received;
- the event parsed against an explicit schema;
- `ingestion_timestamp`.

Nothing is filtered or fixed. A message that is not valid JSON still gets a
row, with NULL parsed fields. Bronze is the replayable record of what arrived.

## 4. Silver: `silver.orders` and `silver.invalid_orders`

1. **Flatten** (`flatten_orders`): the nested payload becomes typed columns.
   Timestamps are parsed with `try_to_timestamp`, so an unparseable one becomes
   NULL rather than failing the run.
2. **De-duplicate** (`deduplicate_events`): rows with the same de-duplication
   key within a one-day arrival window are reduced to one. The key is
   `event_id`, or a hash of the raw text when a message has no `event_id`.
3. **Validate** (`core/quality.py`): ten named rules. A row that passes all of
   them goes to `silver.orders`. A row that fails any goes to
   `silver.invalid_orders` with a `validation_errors` column listing the rules
   it broke, and its `raw_json` for debugging.

`silver.orders` has Delta Change Data Feed enabled and one row per *event*: an
order that was created, paid and shipped has three rows.

## 5. CDF merge: `silver.current_orders`

`jobs/cdf_merge.py` turns the event history into one row per *order*.

1. Read the last processed Delta version of `silver.orders` from
   `metadata.pipeline_state`.
2. Read the change feed for the versions after it.
3. Keep the newest event per order, by event time
   (`core/cdc.py::latest_change_per_order`).
4. MERGE into `silver.current_orders` on `order_id`. An existing row is
   overwritten only if the incoming event is newer.
5. Save the new position, only after the MERGE succeeded.

## 6. Gold

`core/metrics.py`.

- `gold.order_activity_by_status`: streaming count of events per one-minute
  event-time window and status, from `silver.orders`. Windows stay open for
  10 minutes of lateness, then are written once.
- `gold.customer_metrics`: orders, lifetime value, average order value, last
  purchase and distinct products per customer.
- `gold.product_metrics`: units sold, revenue, orders, distinct customers and
  average unit price per product.

The two metric tables are built from `silver.current_orders`, so each order is
counted once, and only when its status is `PAID`, `SHIPPED` or `DELIVERED`.

## 7. Validation

`jobs/validate_outputs.py` runs six checks and fails the run if any does not hold:

| Check | Proves |
|---|---|
| distinct Bronze events = Silver valid + Silver invalid | nothing lost or double-counted in Silver |
| `current_orders` rows = distinct orders = distinct orders in Silver | exactly one row per order |
| no order's stored state is older than its newest event | out-of-order events were merged correctly |
| customer metrics orders and revenue = the same totals from `current_orders` | Gold agrees with its source |
| product metrics orders and revenue = the same totals from `current_orders` | Gold agrees with its source |
| `current_orders` is not empty | the run produced output |
