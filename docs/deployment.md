# Deploying to Azure

The Azure resources are deleted between demos to avoid cost, so this is a
from-scratch checklist. Resource names are examples; the ones the code depends
on are called out. It was followed on a real subscription; the result, and the
problems found, are in [azure-run.md](azure-run.md). Everything in sections 1
and 3 can also be done in the Azure portal; sections 2 and 4 need the
Databricks CLI.

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

Names the pipeline expects (set in `resources/shopsphere_data_platform_etl.pipeline.yml`),
which must match exactly: event hub `orders`, access policy
`shopsphere-databricks-consumer`, and a consumer group `databricks-shopstream`
on the `orders` hub:

```bash
az eventhubs eventhub consumer-group create -g $RG --namespace-name $NS \
  --eventhub-name orders -n databricks-shopstream
```

## 2. Databricks: catalog and secret

```bash
databricks auth login --host https://<workspace>.azuredatabricks.net --profile shopsphere
export DATABRICKS_CONFIG_PROFILE=shopsphere
```

Without the profile, the CLI uses the `DEFAULT` entry of `~/.databrickscfg`,
which may still point at an old workspace. Always check with
`databricks current-user me`.

Create the catalog once, in a SQL editor:

```sql
CREATE CATALOG IF NOT EXISTS adb_shopsphere_dev;
```

Catalogs belong to the regional Unity Catalog metastore, not to a workspace.
A catalog from a previous deployment may still exist, with its storage gone. If
the name is taken, choose another and pass it as `--var="catalog=<name>"` when
deploying.

Store the read-only Event Hubs key in a secret scope:

```bash
KEY=$(az eventhubs namespace authorization-rule keys list -g $RG --namespace-name $NS \
  -n shopsphere-databricks-consumer --query primaryKey -o tsv)

databricks secrets create-scope shopsphere-eventhubs --profile shopsphere
databricks secrets put-secret shopsphere-eventhubs consumer-shared-access-key \
  --string-value "$KEY" --profile shopsphere
```

The secret is the access policy's **primary key**, not the connection string;
the pipeline builds the connection string itself. The Databricks UI cannot
create this kind of scope (it only creates Key Vault-backed ones), so the CLI is
required.

## 3. Data Factory and its identity

```bash
az extension add --name datafactory
az datafactory create -g $RG -n adf-shopsphere -l $LOC

# Application ID of the factory's managed identity
PRINCIPAL=$(az datafactory show -g $RG -n adf-shopsphere --query identity.principalId -o tsv)
ADF_APP_ID=$(az ad sp show --id $PRINCIPAL --query appId -o tsv)
```

Add that identity to the Databricks workspace as a service principal
(Settings → Identity and access → Service principals → Add, **Microsoft Entra
ID managed**, using `$ADF_APP_ID`). The bundle then grants it permission to run
the pipeline and the two jobs. In the portal the same ID is the **Application
ID** (not the Object ID) of the factory's managed identity under Microsoft
Entra ID → Enterprise applications, with the *Managed Identities* filter.

Also give the factory's identity the **Contributor** role on the Databricks
workspace resource (Access control (IAM) → Add role assignment). The linked
service in step 6 needs it.

## 4. Deploy the bundle

```bash
cd databricks_bundle
databricks bundle validate -t dev --var="catalog=adb_shopsphere_dev" --var="eventhubs_namespace=$NS" --var="adf_service_principal_id=$ADF_APP_ID"
databricks bundle deploy   -t dev --var="catalog=adb_shopsphere_dev" --var="eventhubs_namespace=$NS" --var="adf_service_principal_id=$ADF_APP_ID"
databricks bundle summary  -t dev --var="catalog=adb_shopsphere_dev" --var="eventhubs_namespace=$NS" --var="adf_service_principal_id=$ADF_APP_ID"
```

Each value is quoted separately, with no comments on the line: zsh does not
split a variable holding several flags, and passes `# ...` to the command. The
`dev` target does not name a workspace; it comes from the profile.
`uv` must be installed, because the bundle builds the wheel with it.

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

# connectivity test, then the real batch (event times spread over the last 30 minutes)
python -m ingestion.run_producer --target eventhubs --max-events 20 --backfill-minutes 5 --seed 1
python -m ingestion.run_producer --target eventhubs --max-events 3000 --backfill-minutes 30 --seed 42
```

`.env` is git-ignored. Each of its three lines must be replaced in full: leaving
the placeholder namespace makes the producer fail with a DNS error. The
producer connection string is the send-only policy's **Connection
string–primary key**. Keep the generator's final `Done: {...}` line: its counts
are what the pipeline's tables should reproduce.

## 6. Data Factory pipeline

In Data Factory Studio:

1. Manage → Linked services → New → **Azure Databricks** on the *Compute*
   tab (not *Azure Databricks Delta Lake*), named exactly
   `LS_DATABRICKS_SHOPSPHERE`, authenticating with the system-assigned managed
   identity and choosing the workspace from the subscription. The form asks
   for a cluster although the activities in this pipeline never use one: choose
   *New job cluster* with placeholder values and do not create a real cluster.
2. Author → create a pipeline, **rename it to `PL_SHOPSPHERE_MEDALLION` in the
   Properties pane first** (the code editor refuses renames), then open the
   `{}` code view and paste the contents of
   `orchestration/adf/PL_SHOPSPHERE_MEDALLION.json`.
3. Run it with **Debug** or **Trigger now**, supplying the parameters (replace
   the whole default in each box, including the workspace URL):

| Parameter | Value |
|---|---|
| `databricksWorkspaceUrl` | `https://<workspace>.azuredatabricks.net` (no trailing slash) |
| `pipelineId` | pipeline ID from `bundle summary` |
| `cdfMergeJobId` | ID of `shopsphere_cdf_merge` |
| `validationJobId` | ID of `shopsphere_validation` |
| `catalog` | `adb_shopsphere_dev` |

Without Data Factory, the same sequence can be run by hand:

1. In the pipeline page, **Select tables for refresh**: `order_events`,
   `orders`, `invalid_orders` (a normal refresh, not a full refresh).
2. Run the `shopsphere_cdf_merge` job.
3. Select tables for refresh again: the three `gold` tables.
4. Run the `shopsphere_validation` job. Every line should read `[PASS]`.

## 7. Tear down

Capture screenshots first; deletion is permanent.

```sql
DROP CATALOG IF EXISTS adb_shopsphere_dev CASCADE;   -- run in Databricks, before deleting the workspace
```

```bash
az group delete -n $RG
```

Dropping the catalog matters because catalogs live in the regional metastore
and would otherwise survive the workspace (see section 2). Deleting the
resource group removes Event Hubs, the workspace (and its managed resource
group) and Data Factory. Check afterwards that no `databricks-rg-...` group is
left, and delete the local `.env`, whose key no longer exists.

## First-run notes

- The first full pipeline update (all tables at once) fails on the Gold
  materialized views, because `silver.current_orders` does not exist until
  the CDF merge job has run once. Follow the stage order above.
- The first CDF merge run reads a snapshot of `silver.orders` rather than its
  change feed from version 0: a Lakeflow-created table has no change data for
  its first version. Later runs are incremental.
- The pipeline imports the project through `shopsphere.source_path` in its
  configuration, which the bundle sets; do not remove it.
- After a **full refresh** of `silver.orders`, the CDF merge job stops with a
  message explaining how to reset its saved position. That is intended: see
  [design decisions](design-decisions.md#why-keep-a-hand-written-cdf-merge).
