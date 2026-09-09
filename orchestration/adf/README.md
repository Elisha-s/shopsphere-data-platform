# ADF Orchestration

`PL_SHOPSPHERE_MEDALLION` orchestrates the Databricks medallion pipeline end-to-end:

Bronze refresh → Silver refresh → CDF merge → Gold refresh → Validation

## Why two different Databricks activity types

- **Lakeflow pipeline refreshes (Bronze/Silver/Gold)** use `WebActivity` calling the
  Lakeflow Pipelines REST API (`POST /api/2.0/pipelines/{id}/updates`), followed by an
  `Until` loop that polls `GET /updates/{update_id}` every 15 seconds until the update
  reaches a terminal state. This exists because ADF has no native activity type that
  waits on a Lakeflow Declarative Pipeline update — only WebActivity can call the API.
- **`cdf_merge` and `validate_outputs`** use the native `DatabricksJob` activity, because
  they're regular Databricks Jobs (Python wheel tasks), and ADF can track job-run
  completion natively without a manual poll loop.

## Failure handling

Each stage (`Bronze`, `Silver`, `CDF/MERGE`, `Validation`) has its own `Fail` activity with
a distinct error code, so a failed run points directly at the stage that broke rather than
a generic pipeline failure.

## Known gaps (not yet addressed)

- Workspace URL, pipeline ID, and job IDs are hardcoded rather than parameterized via
  ADF global parameters — fine for a single-environment portfolio project, would need
  parameterization for dev/prod in production.
- No trigger is currently attached (manual run only) and no failure notification
  (Teams/email) fires on the `Fail` activities.