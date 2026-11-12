# Deploying to Azure

The Azure resources are deleted between demos to avoid cost, so this is a
from-scratch checklist. Resource names are examples; the ones the code depends
on are called out.

## What you need

- An Azure subscription, and the Azure CLI (`az login`)
- The Databricks CLI (with bundle support) and `uv`
- A Databricks workspace on the Premium tier with Unity Catalog enabled

## 1. Event Hubs

The Kafka endpoint requires the **Standard** tier or higher.

```bash
RG=rg-shopsphere
LOC=centralindia
NS=shopsphere-events          # must be globally unique; pass it to the bundle if you change it

az group create -n $RG -l $LOC
az eventhubs namespace create -g $RG -n $NS -l $LOC --sku Standard
az eventhubs eventhub create -g $RG --namespace-name $NS -n orders --partition-count 2

# Two access policies: one that can only read (Databricks), one that can only send (producer)
az eventhubs namespace authorization-rule create -g $RG --namespace-name $NS \
  -n shopsphere-databricks-consumer --rights Listen
az eventhubs namespace authorization-rule create -g $RG --namespace-name $NS \
  -n shopsphere-producer --rights Send
```

Names the pipeline expects (set in `resources/shopsphere_data_platform_etl.pipeline.yml`):
event hub `orders`, access policy `shopsphere-databricks-consumer`.

## 2. Databricks: catalog and secret

```bash
databricks auth login --host https://<workspace>.azuredatabricks.net --profile shopsphere-dev
```

Create the catalog once, in a SQL editor:

```sql
CREATE CATALOG IF NOT EXISTS adb_shopsphere_dev;
```

Store the read-only Event Hubs key in a secret scope:

```bash
KEY=$(az eventhubs namespace authorization-rule keys list -g $RG --namespace-name $NS \
  -n shopsphere-databricks-consumer --query primaryKey -o tsv)

databricks secrets create-scope shopsphere-eventhubs --profile shopsphere-dev
databricks secrets put-secret shopsphere-eventhubs consumer-shared-access-key \
  --string-value "$KEY" --profile shopsphere-dev
```

## 3. Data Factory and its identity

```bash
az extension add --name datafactory
az datafactory create -g $RG -n adf-shopsphere -l $LOC

# Application ID of the factory's managed identity
PRINCIPAL=$(az datafactory show -g $RG -n adf-shopsphere --query identity.principalId -o tsv)
ADF_APP_ID=$(az ad sp show --id $PRINCIPAL --query appId -o tsv)
```

Add that identity to the Databricks workspace as a service principal
(Settings → Identity and access → Service principals → Add, using
`$ADF_APP_ID`). The bundle then grants it permission to run the pipeline and
the two jobs.

## 4. Deploy the bundle

```bash
cd databricks_bundle
databricks bundle validate -t dev --profile shopsphere-dev --var adf_service_principal_id=$ADF_APP_ID
databricks bundle deploy   -t dev --profile shopsphere-dev --var adf_service_principal_id=$ADF_APP_ID
databricks bundle summary  -t dev --profile shopsphere-dev --var adf_service_principal_id=$ADF_APP_ID
```

If your namespace is not `shopsphere-events`, add `--var eventhubs_namespace=<name>`.

`bundle summary` prints the pipeline ID and the two job IDs. Note them for step 6.

For production, create `adb_shopsphere_prod` and deploy with `-t prod`. Prod
uses a different catalog, so it shares no tables with dev.

## 5. Send events

```bash
cp .env.example .env
# EVENTHUB_NAMESPACE=<your namespace>
# EVENTHUB_PRODUCER_CONNECTION_STRING=<primaryConnectionString of shopsphere-producer>
az eventhubs namespace authorization-rule keys list -g $RG --namespace-name $NS \
  -n shopsphere-producer --query primaryConnectionString -o tsv

python -m ingestion.run_producer --target eventhubs --rate 5 --max-events 3000
```

## 6. Data Factory pipeline

In Data Factory Studio:

1. Create a linked service of type Azure Databricks named
   `LS_DATABRICKS_SHOPSPHERE`, authenticating with the managed identity.
2. Author → Pipelines → import `orchestration/adf/PL_SHOPSPHERE_MEDALLION.json`.
3. Run it with **Debug** or **Trigger now**, supplying the parameters:

| Parameter | Value |
|---|---|
| `databricksWorkspaceUrl` | `https://<workspace>.azuredatabricks.net` (no trailing slash) |
| `pipelineId` | pipeline ID from `bundle summary` |
| `cdfMergeJobId` | ID of `shopsphere_cdf_merge` |
| `validationJobId` | ID of `shopsphere_validation` |
| `catalog` | `adb_shopsphere_dev` |

Without Data Factory, the same sequence can be run by hand: refresh the Bronze
and Silver tables in the pipeline UI, run `shopsphere_cdf_merge`, refresh the
Gold tables, run `shopsphere_validation`.

## 7. Tear down

```bash
databricks bundle destroy -t dev --profile shopsphere-dev --var adf_service_principal_id=$ADF_APP_ID
az group delete -n $RG
```

## First-run notes

- The first full pipeline update (all tables at once) fails on the Gold
  materialized views, because `silver.current_orders` does not exist until
  the CDF merge job has run once. Follow the stage order above.
- After a **full refresh** of `silver.orders`, the CDF merge job stops with a
  message explaining how to reset its saved position. That is intended: see
  [design decisions](design-decisions.md#why-keep-a-hand-written-cdf-merge).
