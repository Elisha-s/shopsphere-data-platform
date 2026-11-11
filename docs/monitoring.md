# Monitoring

## Validation report (both runtimes)

The last stage of every run prints one line per reconciliation check and
fails the run if any check fails. Locally it is in the runner's output; on
Databricks it is in the `shopsphere_validation` job run's output, and a
failure surfaces in Data Factory as `SHOPSPHERE_VALIDATION_FAILED`.

## Quarantine table (both runtimes)

Why records were rejected:

```sql
SELECT validation_errors, count(*) AS records
FROM silver.invalid_orders        -- on Databricks: <catalog>.silver.invalid_orders
GROUP BY validation_errors
ORDER BY records DESC;
```

Each row keeps its `raw_json`, so a rejected record can be inspected exactly
as it arrived.

## CDF merge position (both runtimes)

```sql
SELECT * FROM metadata.pipeline_state;
```

`last_processed_version` is the newest Delta version of `silver.orders` that
has been merged into `silver.current_orders`, and `updated_at` is when.

## Lakeflow event log (Azure)

The pipeline publishes its event log as `<catalog>.bronze.pipeline_event_log`
(set under `event_log` in the pipeline resource). It records every update,
each flow's progress, and how many rows passed and failed each expectation.

Rows passed and failed per data-quality rule, per update:

```sql
SELECT
  origin.update_id,
  e.name            AS rule,
  sum(e.passed_records) AS passed,
  sum(e.failed_records) AS failed
FROM (
  SELECT
    origin,
    explode(from_json(
      details:flow_progress.data_quality.expectations,
      'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>'
    )) AS e
  FROM adb_shopsphere_dev.bronze.pipeline_event_log
  WHERE event_type = 'flow_progress'
    AND details:flow_progress.data_quality.expectations IS NOT NULL
)
GROUP BY origin.update_id, e.name
ORDER BY origin.update_id, rule;
```

Recent update outcomes:

```sql
SELECT timestamp, origin.update_id, details:update_progress.state AS state
FROM adb_shopsphere_dev.bronze.pipeline_event_log
WHERE event_type = 'update_progress'
ORDER BY timestamp DESC
LIMIT 20;
```

## Data Factory run history (Azure)

Monitor → Pipeline runs → `PL_SHOPSPHERE_MEDALLION` shows each activity's
status and duration. A failed run carries one of these error codes:

| Error code | Meaning |
|---|---|
| `SHOPSPHERE_BRONZE_FAILED`, `SHOPSPHERE_SILVER_FAILED`, `SHOPSPHERE_GOLD_FAILED` | That pipeline refresh ended as FAILED or CANCELED |
| `SHOPSPHERE_BRONZE_TIMEOUT`, `SHOPSPHERE_SILVER_TIMEOUT`, `SHOPSPHERE_GOLD_TIMEOUT` | The refresh did not finish in 30 minutes, or its status could not be read |
| `SHOPSPHERE_CDF_MERGE_FAILED` | The CDF merge job failed |
| `SHOPSPHERE_VALIDATION_FAILED` | A reconciliation check failed |

## Known gaps

- No alert is sent on failure. Failures are visible in run history only. The
  next step would be an alert rule on failed Data Factory runs, or email
  notifications on the two jobs.
- No trigger is attached to the Data Factory pipeline; runs are started manually.
