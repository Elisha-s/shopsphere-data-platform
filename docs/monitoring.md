# Monitoring

## Lakeflow event log
Pipeline run history, table update metrics, and data-quality expectation results are
available in:
```text
adb_shopsphere_dev.monitoring.pipeline_runs
```

Example: check how many records failed each Silver expectation in the last run
```sql
SELECT expectations
FROM adb_shopsphere_dev.monitoring.pipeline_runs
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC
LIMIT 1;
```

## Reconciliation
`validate_outputs.py` (run as the final ADF/Databricks job stage) reports:
- Bronze record count vs. unique event count (duplicates removed)
- Silver valid vs. invalid counts
- Confirms no gold table is empty

A failed reconciliation raises a `RuntimeError` with the specific counts, which fails
the ADF run at the `FAIL_VALIDATION` stage.

## ADF run history
Pipeline run status, per-activity duration, and failure messages are visible in Data
Factory Studio under Monitor → Pipeline runs, filtered to `PL_SHOPSPHERE_MEDALLION`.

## Known gap
No automated alerting currently fires on failure (Teams/email) — failures are visible
only by checking ADF/Databricks run history manually.