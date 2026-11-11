# Architecture

ShopSphere processes a stream of e-commerce order events into trusted,
analytics-ready tables. It has one transformation library and two runtimes.

## Components

| Component | Local runtime | Azure runtime |
|---|---|---|
| Event source | `ingestion/` generator | Same generator |
| Message transport | Kafka (Docker) or JSON files in a landing folder | Azure Event Hubs, read through its Kafka endpoint |
| Stream processing | Spark Structured Streaming 3.5 | Lakeflow Declarative Pipelines (serverless) |
| Storage format | Delta Lake 3.2 on local disk | Delta Lake in Unity Catalog managed tables |
| Table catalog | Embedded metastore in the data folder | Unity Catalog (`<catalog>.<schema>.<table>`) |
| Current-state merge | `jobs/cdf_merge.py`, called by the runner | The same file, as a Databricks job (Python wheel task) |
| Validation | `jobs/validate_outputs.py`, called by the runner | The same file, as a Databricks job |
| Orchestration | `local_pipeline/run_pipeline.py` | Azure Data Factory `PL_SHOPSPHERE_MEDALLION` |
| Deployment | `pip install` / Docker Compose | Databricks Asset Bundle (`databricks_bundle/`) |
| Secrets | none needed | Databricks secret scope (Event Hubs key) |

## The shared library

`databricks_bundle/src/shopsphere_data_platform_etl/core/` holds every
business rule. Its functions take DataFrames and return DataFrames, and know
nothing about where they run.

| Module | Responsibility |
|---|---|
| `schemas.py` | Explicit schema of an order event |
| `orders.py` | Bronze parsing, Silver flattening, de-duplication |
| `quality.py` | The rule set that defines a valid order |
| `cdc.py` | "Latest event per order" and the MERGE conditions |
| `metrics.py` | Gold aggregations |
| `tables.py` | Table names, with or without a catalog prefix |

The Lakeflow files in `transformations/` only declare tables and call `core`.
The local runner in `local_pipeline/stages.py` calls the same functions and
writes the results with ordinary streaming writes. This is what makes the
logic unit-testable without Databricks, and it means the end-to-end test of
the local pipeline is also a test of the Azure pipeline's logic.

## Tables

| Layer | Table | Grain | Written by |
|---|---|---|---|
| Bronze | `bronze.order_events` | one row per delivered message | streaming ingest |
| Silver | `silver.orders` | one row per valid, distinct event | streaming |
| Silver | `silver.invalid_orders` | one row per rejected, distinct event | streaming |
| Silver | `silver.current_orders` | one row per order (latest state) | CDF merge job |
| Gold | `gold.order_activity_by_status` | one row per minute and status | streaming aggregation |
| Gold | `gold.customer_metrics` | one row per customer | materialized view / batch overwrite |
| Gold | `gold.product_metrics` | one row per product | materialized view / batch overwrite |
| Metadata | `metadata.pipeline_state` | one row per incremental job | CDF merge job |

## Orchestration order

```text
Bronze refresh -> Silver refresh -> CDF merge job -> Gold refresh -> Validation job
```

`silver.current_orders` is written by a job, outside the Lakeflow pipeline,
and the Gold materialized views read it. The pipeline therefore cannot be
refreshed in one go: the merge has to run between Silver and Gold. Azure Data
Factory enforces that with three selective pipeline refreshes around two job
runs. See [`orchestration/adf/README.md`](../orchestration/adf/README.md).

## Environments

Dev and prod are separated by Unity Catalog catalog (`adb_shopsphere_dev`,
`adb_shopsphere_prod`). The catalog is a bundle variable passed to the
pipeline as configuration and to both jobs as a `--catalog` parameter, so no
table name is hardcoded to an environment.
