# Design decisions and trade-offs

Each section states the decision, the reason, and what was given up.

## Why two runtimes

The first version ran on self-managed Kafka and Spark in Docker. It proved
the medallion design, but running it meant owning the broker, the Spark
process lifecycle and the checkpoint storage. The Azure version hands those to
managed services: Event Hubs for transport, Lakeflow for stream execution,
checkpoints and data-quality metrics, Unity Catalog for governance, Data
Factory for scheduling across systems.

The local runtime was kept because it costs nothing to run, works offline, and
makes the logic testable in CI. To stop the two from drifting, the business
logic lives in one library (`core/`) that both call.

**Trade-off:** the local runner is not Lakeflow. It reproduces the same tables
with ordinary streaming writes, so Lakeflow-specific behaviour (expectation
metrics, materialized view refresh) is only exercised on Databricks.

## Why Event Hubs through its Kafka endpoint

The producer and the Spark reader both speak the Kafka protocol, so moving
from Kafka to Event Hubs changed connection settings, not code. The same
`OrderEventProducer` class serves both.

**Trade-off:** authentication uses a shared access key from a Databricks
secret scope. Microsoft Entra ID (managed identity) would avoid a long-lived
key and is the stronger choice for production.

## Bronze keeps everything, including broken messages

Bronze stores the raw JSON and the transport metadata and rejects nothing. If
a parsing rule or a quality rule changes, Silver can be rebuilt from Bronze
without asking the source to resend. Quality decisions are made once, in
Silver, where they are visible and counted.

## One rule set, used three ways

`ORDER_RULES` in `core/quality.py` is the only definition of a valid order. It
feeds the Lakeflow expectations on `silver.orders`, the filter for
`silver.invalid_orders`, and the `validation_errors` text. Two hand-written
copies of the rules would eventually disagree.

Each rule is written to evaluate to TRUE or FALSE, never NULL. In SQL,
`quantity > 0` is NULL when `quantity` is NULL, and a row with a NULL
condition passes neither a filter nor its negation. Such rows would vanish
from both tables. `coalesce(quantity > 0, false)` makes them fail the rule and
land in quarantine.

## De-duplication: bounded state, arrival-time watermark

`dropDuplicatesWithinWatermark` on a one-day watermark.

- A plain `dropDuplicates(["event_id"])` on a stream remembers every id it has
  ever seen, so its state grows without limit. The within-watermark variant
  forgets ids older than the window.
- The watermark is on arrival time, not business event time. Duplicates come
  from redelivery, so they arrive close together. An arrival-time watermark
  also never discards a row for carrying an old business timestamp.

**Trade-off:** a duplicate that arrives more than a day after the original is
not removed in Silver. The merge into `current_orders` is still unaffected by
it, and the Bronze-to-Silver reconciliation check would flag it.

## Why keep a hand-written CDF merge

`silver.current_orders` could be produced inside the pipeline with Lakeflow's
built-in change-data-capture flow. It is a separate job here on purpose: the
project is meant to show how incremental processing works underneath.

- **Incremental:** only unprocessed Delta versions are read, tracked in a
  state table. No full rebuild.
- **Correct under late data:** "newest" is decided by event time, and the
  MERGE only overwrites with a newer event.
- **Idempotent:** the position is saved after the MERGE. A crash in between
  replays the same versions, and the guarded MERGE makes the replay a no-op.
- **Fails loudly:** if the saved position is ahead of the table (the table was
  recreated), the job stops with instructions instead of reporting "nothing
  new" forever.

**Trade-offs:** more code to own than the built-in flow; the MERGE and the
position update are two transactions, not one; and because the job sits
outside the pipeline, the pipeline has to be refreshed in three parts.

## Late data, layer by layer

| Layer | Policy |
|---|---|
| Silver | Never dropped for lateness (arrival-time watermark). |
| `current_orders` | Applied if newer than the stored state, ignored if older. |
| `order_activity_by_status` | Counted if it arrives within 10 minutes of its window; later than that it is left out of this operational view. |

The generator's late events are 7 minutes late, inside the 10-minute
allowance. The cost of a longer allowance is that windows are published later.

## Revenue counts paid orders only

Customer and product metrics include an order only when its current status is
`PAID`, `SHIPPED` or `DELIVERED`. A `CREATED` order may never be paid, and a
`CANCELLED` one is not revenue. Because the metrics read `current_orders`, an
order that moves from `CREATED` to `PAID` starts counting on the next refresh.

## Dev and prod are separated by catalog

Schema names (`bronze`, `silver`, `gold`, `metadata`) are the same in every
environment and the catalog differs. One variable switches the environment,
and prod cannot write to dev tables.

## Orchestrating Lakeflow from Data Factory

Data Factory has a native activity for Databricks jobs but none that waits for
a Lakeflow pipeline update, so pipeline refreshes are started through the REST
API and polled until they finish. Each stage has its own failure activity and
error code, so a failed run names the stage that broke.
