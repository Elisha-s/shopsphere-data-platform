# Deployment

ShopSphere's Databricks resources are managed using Declarative (Asset) Bundles.

## Validate
```bash
cd databricks_bundle
databricks bundle validate -t dev --profile shopsphere-dev
```

## Deploy
```bash
databricks bundle deploy -t dev --profile shopsphere-dev
```

Deploy to prod the same way with `-t prod`. Bundle targets (`dev`/`prod`) set the
catalog/schema variables and workspace permissions — see `databricks_bundle/databricks.yml`.

## Orchestration deployment
The ADF pipeline (`orchestration/adf/PL_SHOPSPHERE_MEDALLION.json`) is imported into
Data Factory Studio manually (Author → Pipelines → Import from ARM/JSON) or via
`az datafactory pipeline create` referencing the JSON file. It has no attached trigger
by design during development — runs are manual/on-demand.

## v1 (local) deployment
```bash
cd legacy/local_spark_v1
docker compose -f docker/docker-compose.yml up
```
This starts Kafka, Kafka UI, the event producer, and the bronze/silver/gold Spark jobs
as separate containers with explicit `depends_on` ordering.