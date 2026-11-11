# ShopSphere Databricks bundle

The Azure Databricks side of ShopSphere, deployed as a Databricks Asset
Bundle. The project overview is in the [repository README](../README.md).

## What is in here

```text
databricks_bundle/
├── databricks.yml                 bundle definition: variables, dev and prod targets
├── pyproject.toml                 the Python package (built into a wheel for the jobs)
├── resources/
│   ├── shopsphere_data_platform_etl.pipeline.yml   Lakeflow pipeline
│   ├── cdf_merge.job.yml                            CDF merge job
│   └── shopsphere_validation.job.yml                validation job
└── src/shopsphere_data_platform_etl/
    ├── core/                      shared business logic (also used by the local pipeline)
    ├── transformations/           Lakeflow table definitions
    │   ├── bronze_orders.py       Event Hubs -> bronze.order_events
    │   ├── silver_orders.py       -> silver.orders, silver.invalid_orders
    │   └── gold_metrics.py        -> gold.*
    └── jobs/
        ├── cdf_merge.py           silver.orders change feed -> silver.current_orders
        └── validate_outputs.py    reconciliation checks
```

## How the three pieces relate

- **The bundle** (`databricks.yml`, `resources/`) is deployment: it creates
  the pipeline and the jobs in a workspace and sets their configuration.
- **The Lakeflow pipeline** runs the files in `transformations/`. Each file
  declares tables with `@dp.table` or `@dp.materialized_view`; Lakeflow works
  out the dependency order, manages checkpoints and records data-quality
  results.
- **The wheel** packages `src/` so the two jobs can run `cdf-merge` and
  `validate-outputs` as Python wheel tasks. The pipeline installs the same
  package, which is how `transformations/` imports `core`.

## Configuration flow

```text
databricks.yml variables (catalog, eventhubs_namespace, adf_service_principal_id)
        │
        ├─> pipeline `configuration`  ─> spark.conf.get("shopsphere.catalog") in transformations/
        └─> job task `parameters`     ─> --catalog argument of cdf-merge / validate-outputs
```

`core/tables.py` turns the catalog into full table names, so no table name is
tied to an environment.

## Commands

```bash
databricks bundle validate -t dev --profile <profile> --var adf_service_principal_id=<id>
databricks bundle deploy   -t dev --profile <profile> --var adf_service_principal_id=<id>
databricks bundle summary  -t dev --profile <profile> --var adf_service_principal_id=<id>

databricks bundle run shopsphere_cdf_merge  -t dev --profile <profile> --var adf_service_principal_id=<id>
databricks bundle run shopsphere_validation -t dev --profile <profile> --var adf_service_principal_id=<id>
```

Full setup, including Event Hubs, secrets and Data Factory:
[`docs/deployment.md`](../docs/deployment.md).

## Execution order

```text
1. Refresh bronze.order_events                      (pipeline)
2. Refresh silver.orders, silver.invalid_orders     (pipeline)
3. Run shopsphere_cdf_merge                         (job)
4. Refresh gold.*                                   (pipeline)
5. Run shopsphere_validation                        (job)
```

Step 3 sits between two pipeline refreshes because `silver.current_orders` is
written by the job and read by the Gold views. Azure Data Factory runs this
sequence: see [`orchestration/adf/README.md`](../orchestration/adf/README.md).

## Tests

The logic in `core/` and `jobs/` is tested from the repository root with
`pytest` and `pytest -m delta`; see [`docs/testing.md`](../docs/testing.md).
