# ADF orchestration

`PL_SHOPSPHERE_MEDALLION.json` runs the Azure pipeline end to end:

```text
Bronze refresh -> Silver refresh -> CDF merge job -> Gold refresh -> Validation job
```

## Why the pipeline is refreshed in three parts

`silver.current_orders` is written by the CDF merge job, outside the Lakeflow
pipeline, and the Gold materialized views read it. The merge must therefore
run after Silver and before Gold. Each refresh uses `refresh_selection` to
update only that layer's tables.

## Two kinds of activity

- **Pipeline refreshes** (Bronze, Silver, Gold) use a `WebActivity` that calls
  the Lakeflow REST API (`POST /api/2.0/pipelines/{id}/updates`), then an
  `Until` loop that polls `GET .../updates/{update_id}` every 15 seconds until
  the state is `COMPLETED`, `FAILED` or `CANCELED`. Data Factory has no native
  activity that waits for a Lakeflow pipeline update.
- **`shopsphere_cdf_merge` and `shopsphere_validation`** use the native
  `DatabricksJob` activity, which waits for the job run by itself.

Authentication is the factory's managed identity in both cases; there is no
token in the definition.

## Failure handling

Every stage has its own `Fail` activity and error code, attached directly to
the activity that can fail:

| Stage | Wrong final state | Timed out / status unreadable |
|---|---|---|
| Bronze | `SHOPSPHERE_BRONZE_FAILED` | `SHOPSPHERE_BRONZE_TIMEOUT` |
| Silver | `SHOPSPHERE_SILVER_FAILED` | `SHOPSPHERE_SILVER_TIMEOUT` |
| CDF merge | `SHOPSPHERE_CDF_MERGE_FAILED` | |
| Gold | `SHOPSPHERE_GOLD_FAILED` | `SHOPSPHERE_GOLD_TIMEOUT` |
| Validation | `SHOPSPHERE_VALIDATION_FAILED` | |

Status polls are retried three times, because they are read-only. The `POST`
that starts a refresh is not retried, because a second start while the first
is running would be rejected.

## Parameters

Nothing environment-specific is hardcoded:

| Parameter | Meaning |
|---|---|
| `databricksWorkspaceUrl` | Workspace URL, no trailing slash |
| `pipelineId` | Lakeflow pipeline ID |
| `cdfMergeJobId`, `validationJobId` | Databricks job IDs |
| `catalog` | Unity Catalog catalog (`adb_shopsphere_dev` or `adb_shopsphere_prod`) |

The pipeline expects a linked service named `LS_DATABRICKS_SHOPSPHERE`.
Setup steps are in [`docs/deployment.md`](../../docs/deployment.md).

## Known gaps

- No trigger is attached; runs are manual.
- No notification is sent when a `Fail` activity fires.
