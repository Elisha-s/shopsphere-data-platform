# How the Azure version works

A plain-English tour of what runs on Azure. You do not need to read the whole
repository: seven files make up the deployed platform. Everything else
(`core/`, the tests, the local runner) supports them.

## The flow in one paragraph

A generator sends order events to **Azure Event Hubs**. A **Databricks
Lakeflow pipeline** reads them with Spark's Kafka source and builds three
layers of **Delta** tables in **Unity Catalog**: Bronze (raw), Silver
(cleaned, de-duplicated, split into valid and quarantined) and Gold
(metrics). A separate job turns the Silver event history into one current row
per order. A second job checks that the layers agree. **Azure Data Factory**
starts these steps in order, and a **Databricks Asset Bundle** deploys the
whole thing from code.

## The seven files

| File | What it does | The one idea to know |
|---|---|---|
| `databricks_bundle/src/.../transformations/bronze_orders.py` | Reads Event Hubs through its Kafka endpoint and stores every event as raw JSON plus transport details | Event Hubs speaks the Kafka protocol, so no special connector is needed. The access key comes from a Databricks secret, never from the code |
| `.../transformations/silver_orders.py` | Flattens the JSON, removes duplicate deliveries, and splits events into `silver.orders` (valid) and `silver.invalid_orders` (failed a rule) | One list of ten rules (`core/quality.py`) decides valid or invalid, so every event lands in exactly one table |
| `.../transformations/gold_metrics.py` | Builds the business tables: per-minute activity, customer metrics, product metrics | Customer and product metrics read `silver.current_orders`, so an order is counted once however many status events it had |
| `.../jobs/cdf_merge.py` | Keeps `silver.current_orders`, one row per order with its latest state | Delta **Change Data Feed** lists only new rows, so the job never re-reads the whole table. A saved version number marks where it stopped |
| `.../jobs/validate_outputs.py` | Compares the layers against each other and fails the run if any numbers disagree | Six checks, for example: Bronze unique events must equal Silver valid plus invalid |
| `databricks_bundle/databricks.yml` and `resources/*.yml` | Declare the pipeline, the two jobs and the dev and prod catalogs | `databricks bundle deploy` creates everything from these files, so there is no clicking in the portal |
| `orchestration/adf/PL_SHOPSPHERE_MEDALLION.json` | The Data Factory pipeline that runs the stages in order | Bronze and Silver, then the merge, then Gold, then validation. ADF authenticates with its own managed identity |

## What the helper code does

The pipeline files are short because the logic lives in
`databricks_bundle/src/.../core/`:

| File | Contains |
|---|---|
| `orders.py` | Parse the raw JSON, flatten it into columns, build the de-duplication key |
| `quality.py` | The ten data-quality rules |
| `cdc.py` | Pick the newest event per order, and the rule for when a merge may overwrite a row |
| `metrics.py` | The Gold calculations |
| `tables.py`, `schemas.py` | Table names and the event schema |

The unit tests call these functions on small hand-made examples. They run on
every push in GitHub Actions. They are a safety net, not something you need
to read to understand the platform.

## Three ideas worth being able to explain

1. **Why duplicates are not a problem.** Event Hubs can deliver an event
   twice. Silver drops repeats using `dropDuplicatesWithinWatermark`, which
   forgets old keys so the state Spark keeps stays small.
2. **Why a late event cannot roll an order back.** The merge only overwrites
   an order when the incoming event is newer by event time, not by arrival
   time.
3. **Why the merge job's first run reads a snapshot.** A table created by a
   Lakeflow pipeline has no change data at version 0, so the first run reads
   the table as it stands, and later runs read the change feed. This was found
   on Azure and is recorded in [azure-run.md](azure-run.md).

## What ran, and what did not

See [azure-run.md](azure-run.md) for the record of the Azure run, and the
"What has been verified" table in the [README](../README.md) for what is
tested automatically and what is not.
