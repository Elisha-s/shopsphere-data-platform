# Testing

## Fast tests: `pytest`

No network access needed. They run on a plain local Spark session.

| File | Covers |
|---|---|
| `tests/unit/test_generator.py` | Seeded catalogues, reproducibility, order lifecycle, unique order ids across runs, duplicate and late injection, file output |
| `tests/unit/test_quality.py` | Every rule, the reasons text, NULL values being quarantined rather than lost, every row landing in exactly one table |
| `tests/unit/test_orders.py` | Bronze keeping raw JSON and metadata, malformed JSON, typed flattening, bad timestamps, the de-duplication key |
| `tests/unit/test_cdc.py` | Latest event per order by event time (not commit order), change types, tie-breaking |
| `tests/unit/test_metrics.py` | Customer and product metrics, revenue statuses |
| `tests/unit/test_local_stages.py` | The real streaming stages (Bronze, Silver, Gold) and the validation job, checked against the counts the generator injected, plus a re-run that must process nothing |

## End-to-end test: `pytest -m delta`

`tests/integration/test_local_pipeline.py` runs the complete local pipeline on
real Delta tables: two batches of data, a re-run with no new data, a simulated
crash between the MERGE and saving the position, and a recreated source table.
It covers what needs Delta: the change feed read, the MERGE and the state table.

It is a separate command because Spark has to download the Delta libraries.

## Continuous integration

`.github/workflows/tests.yml` runs lint, the fast tests and the end-to-end
test on every push and pull request.

## What is not tested automatically

- **Lakeflow-specific behaviour**: the `@dp.table` / `@dp.expect_all_or_drop`
  declarations, materialized view refresh and the Event Hubs connection only
  run on Databricks. The logic they call is tested; the wiring is verified by
  deploying.
- **The Kafka source** of the local pipeline: needs a broker, so it is
  exercised through Docker Compose rather than in CI.
- **The ADF pipeline**: CI checks that the definition is valid JSON. Its
  behaviour is verified by running it in Data Factory.
- **The Asset Bundle**: `databricks bundle validate` needs workspace
  credentials, so it is run by hand before deploying.
