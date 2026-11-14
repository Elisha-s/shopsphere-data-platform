# Record of the Azure run

What was deployed, what it produced, and what went wrong before it worked.
The environment was deleted after the run, so this page is the evidence.

## What was deployed

| Resource | Setting |
|---|---|
| Region | Central India, everything in one resource group |
| Event Hubs | Standard tier (needed for the Kafka endpoint), one hub `orders`, 2 partitions, a send-only and a listen-only access policy |
| Databricks | Premium tier workspace, Unity Catalog, serverless compute for the pipeline and both jobs |
| Catalog | One catalog for the dev target, created by hand |
| Secret | The listen-only key, stored in a Databricks secret scope; nothing secret is in Git |
| Data Factory | V2, system-assigned managed identity, added to Databricks as a service principal |
| Deployment | `databricks bundle deploy -t dev` created the pipeline and the two jobs |

## What was sent

The generator ran twice against Event Hubs: a 20-event connectivity test and a
3,000-tick run with event times spread over the previous 30 minutes. Together
they delivered 3,082 events, of which the generator had deliberately made 54
invalid and 62 duplicates (and 142 late).

## What the pipeline reported

Stages were run in the documented order: Bronze and Silver, the CDF merge job,
Gold, then the validation job.

| Check | Result |
|---|---|
| Bronze rows | 3,082, equal to the events delivered |
| Duplicates removed in Silver | 62, equal to the duplicates injected |
| Unique events | 3,020 |
| Silver valid / invalid | 2,966 / 54, invalid equal to the invalid injected |
| `current_orders` | 1,633 rows, one per distinct order, none holding an older state than its newest event |
| Customer and product metrics | 744 orders each, revenue 78,490,719.00 each, equal to the totals computed from `current_orders` |
| Validation job | All six checks `[PASS]` |

The generator reported 1,650 orders created; 1,633 reached `current_orders`.
The difference was not investigated here. The likely cause is orders whose
events were all rejected as invalid, but that is an explanation, not a result.

## What went wrong, and what changed

| # | Symptom | Cause | Fix |
|---|---|---|---|
| 1 | Pipeline failed to start: `No module named 'shopsphere_data_platform_etl'` | The Lakeflow runtime did not make the project importable through the editable install | The bundle passes `src/` in as `shopsphere.source_path`, and each transformation file adds it to `sys.path` |
| 2 | A full pipeline update failed on the Gold views: `silver.current_orders` not found | The table is written by the merge job, which had not run yet | Not a bug: refresh Bronze and Silver, run the merge, then refresh Gold. This is what the ADF pipeline automates |
| 3 | The first merge job failed with `DELTA_MISSING_CHANGE_DATA` for version 0 | Lakeflow creates the Silver table first and applies its properties afterwards, so the change feed does not exist at version 0. The local tests could not show this, because the local table is created with the property from the start | The first run reads a snapshot of `silver.orders` at its latest version and saves that version. Later runs read the change feed from the next version |
| 4 | Data Factory pipeline could not be created from the JSON, then called a placeholder URL | The editor does not allow a rename; parameters were left at their defaults | Rename first; set the parameter defaults |

Setup mistakes that were not code defects, kept because they will recur:

- A catalog with the planned name already existed. Catalogs live in the
  regional metastore, so one from an earlier, deleted deployment was still
  there. A new name was used through `--var catalog=...`.
- The access policy and consumer group names must match the pipeline
  configuration exactly (`shopsphere-databricks-consumer`, `databricks-shopstream`).
- In zsh, `--var` values must be quoted one by one, and `# comments` on a
  command line are passed as arguments.
- The Data Factory linked service must be **Azure Databricks** (the Compute
  connector), not **Azure Databricks Delta Lake**, which is a copy connector
  and requires a cluster.
- A `.env` file copied from the example keeps its placeholder lines unless each
  one is replaced.

## What this run does not show

- **Throughput.** About 3,000 events is a functional check, not a load test.
- **Long-running behaviour.** The pipeline was run in triggered mode a few times,
  not continuously for days.
- **Production settings.** The `prod` target was not deployed.
- **Failure paths in Data Factory.** The `Fail` activities were not
  provoked.
