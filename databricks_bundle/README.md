# ShopSphere Databricks Bundle

This directory contains the Databricks implementation and deployment configuration for the ShopSphere Data Platform.

The bundle manages the Databricks-side components of the project, including:

- Lakeflow Declarative Pipeline resources
- Bronze, Silver, and Gold transformations
- Delta Change Data Feed processing
- Delta MERGE into the current-state orders table
- validation jobs
- Python wheel packaging
- environment-specific deployment through Databricks Asset Bundles

The repository-level `README.md` describes the complete ShopSphere architecture, including Azure Event Hubs and Azure Data Factory.

---

## Directory Structure

```text
databricks_bundle/
│
├── databricks.yml
├── pyproject.toml
│
├── resources/
│   ├── shopsphere_data_platform_etl.pipeline.yml
│   ├── cdf_merge.job.yml
│   └── shopsphere_validation.job.yml
│
├── src/
│   └── shopsphere_data_platform_etl/
│       ├── transformations/
│       │   ├── eventhub_orders.py
│       │   ├── silver_orders.py
│       │   └── gold_metrics.py
│       │
│       └── jobs/
│           ├── cdf_merge.py
│           └── validate_outputs.py
│
└── tests/
```

---

## Databricks Processing Flow

The Databricks data path is:

```text
Azure Event Hubs
        ↓
eventhub_orders.py
        ↓
Bronze
eventhub_order_events
        ↓
silver_orders.py
        ↓
Silver
orders
   │
   ├──────────────→ invalid_orders
   │
   ├──────────────→ Streaming Gold
   │
   │                 order_activity_by_status
   │
   ↓
Delta Change Data Feed
        ↓
cdf_merge.py
        ↓
Delta MERGE
        ↓
current_orders
   │
   ├──────────────→ customer_metrics
   └──────────────→ product_metrics
        ↓
validate_outputs.py
```

---

## Lakeflow Pipeline

The Lakeflow Declarative Pipeline is defined in:

```text
resources/shopsphere_data_platform_etl.pipeline.yml
```

The transformation code is located under:

```text
src/shopsphere_data_platform_etl/transformations/
```

Lakeflow manages the Bronze, Silver, and Gold datasets declared by these Python files.

---

## Bronze — `eventhub_orders.py`

`eventhub_orders.py` consumes order events from Azure Event Hubs through its Kafka-compatible endpoint using Spark Structured Streaming.

The Bronze table is:

```text
adb_shopsphere_dev.bronze.eventhub_order_events
```

Bronze preserves:

- message key
- raw JSON payload
- Kafka/Event Hubs topic
- partition
- offset
- Event Hubs enqueue timestamp
- parsed event envelope
- ingestion timestamp

The Bronze layer is intended to preserve the original source event and transport metadata for traceability and debugging.

---

## Silver — `silver_orders.py`

`silver_orders.py` reads the Bronze table as a streaming source.

It performs:

- nested payload flattening
- timestamp conversion
- data-quality validation
- invalid-record quarantine
- event-time watermarking
- deduplication by `event_id`

The valid table is:

```text
adb_shopsphere_dev.silver.orders
```

Invalid records are written to:

```text
adb_shopsphere_dev.silver.invalid_orders
```

Delta Change Data Feed is enabled on `silver.orders`.

Silver represents validated event/change history, so the same `order_id` may appear multiple times when an order changes state.

Example:

```text
O100 CREATED
O100 PAID
O100 SHIPPED
```

---

## Incremental Current-State Processing — `cdf_merge.py`

`cdf_merge.py` is a standalone Python wheel job.

Its purpose is to convert Silver event history into one current row per order.

The flow is:

```text
Read latest Silver Delta version
        ↓
Read last processed version
        ↓
Read only new CDF changes
        ↓
Keep inserts and update post-images
        ↓
Choose latest change per order_id
        ↓
MERGE into current_orders
        ↓
Save processed version
```

The target table is:

```text
adb_shopsphere_dev.silver.current_orders
```

Processing state is stored in:

```text
adb_shopsphere_dev.metadata.pipeline_state
```

This prevents the job from repeatedly scanning the complete Silver table.

---

## Gold — `gold_metrics.py`

The Gold layer uses a hybrid design.

### Streaming operational metrics

```text
adb_shopsphere_dev.gold.order_activity_by_status
```

This table reads from:

```text
silver.orders
```

because it intentionally analyzes event activity.

It uses event-time windows and produces metrics such as:

- event count
- approximate unique order count

This means separate lifecycle events such as `CREATED`, `PAID`, and `SHIPPED` are intentionally counted as separate events.

### Current-state business metrics

The following Gold objects read from:

```text
silver.current_orders
```

rather than Silver event history:

```text
adb_shopsphere_dev.gold.customer_metrics
adb_shopsphere_dev.gold.product_metrics
```

This prevents repeated lifecycle events for one order from inflating order counts, revenue, or units sold.

---

## Validation — `validate_outputs.py`

`validate_outputs.py` performs final reconciliation and output checks.

The validation job checks areas such as:

- Bronze/Silver reconciliation
- duplicate-aware row accounting
- invalid-record handling
- existence of `current_orders`
- availability of expected Gold outputs

The job raises an exception when critical validation fails so orchestration can treat the pipeline execution as unsuccessful.

---

## Resource Definitions

Databricks resources are defined under:

```text
resources/
```

### Lakeflow Pipeline

```text
shopsphere_data_platform_etl.pipeline.yml
```

Defines the Bronze, Silver, and Gold Lakeflow pipeline.

### CDF/MERGE Job

```text
cdf_merge.job.yml
```

Runs the wheel entry point:

```text
cdf-merge
```

which executes:

```text
shopsphere_data_platform_etl.jobs.cdf_merge:main
```

### Validation Job

```text
validation.job.yml
```

Runs the wheel entry point:

```text
validate-outputs
```

which executes:

```text
shopsphere_data_platform_etl.jobs.validate_outputs:main
```

---

## Python Wheel Packaging

Python packaging is configured in:

```text
pyproject.toml
```

The package included in the wheel is:

```text
src/shopsphere_data_platform_etl
```

The relevant entry points are:

```toml
[project.scripts]
cdf-merge = "shopsphere_data_platform_etl.jobs.cdf_merge:main"
validate-outputs = "shopsphere_data_platform_etl.jobs.validate_outputs:main"
```

Conceptually:

```text
Python source
      ↓
pyproject.toml
      ↓
Wheel build
      ↓
.whl artifact
      ↓
Installed in Databricks job environment
      ↓
Entry point executed
```

A Python wheel packages the application code.

The Asset Bundle defines how that code and the Databricks resources are deployed.

---

## Databricks Asset Bundle

The bundle is configured through:

```text
databricks.yml
```

It defines:

- bundle identity
- workspace targets
- deployment variables
- resource YAML inclusion
- Python wheel artifact build configuration

The bundle currently supports development and production targets.

---

## Configuration Flow

Pipeline configuration is supplied through the Databricks resource definition.

For example:

```text
bundle variable
      ↓
pipeline configuration
      ↓
Spark configuration
      ↓
Python
```

Transformation code reads configuration using:

```python
spark.conf.get("shopsphere.catalog")
```

Similar Spark configuration entries are used for Event Hubs namespace, topic, consumer group, and secret references.

Sensitive Event Hubs credentials are retrieved from Databricks Secrets rather than stored directly in source code.

---

## Local Development

From this directory:

```bash
cd databricks_bundle
```

Build the Python package:

```bash
python -m build
```

The generated wheel is written under:

```text
dist/
```

---

## Validate the Bundle

```bash
databricks bundle validate \
  -t dev \
  --profile shopsphere-dev
```

Validation checks the bundle configuration but does not run the data pipeline.

---

## Deploy the Bundle

```bash
databricks bundle deploy \
  -t dev \
  --profile shopsphere-dev
```

Deployment creates or updates the configured Databricks resources.

---

## Inspect Deployed Resources

```bash
databricks bundle summary \
  -t dev \
  --profile shopsphere-dev
```

This can be used to inspect deployed jobs, pipelines, and associated resource identifiers.

---

## Run Individual Jobs

To run only the incremental current-state job:

```bash
databricks bundle run shopsphere_cdf_merge \
  -t dev \
  --profile shopsphere-dev
```

To run only output validation:

```bash
databricks bundle run shopsphere_validation \
  -t dev \
  --profile shopsphere-dev
```

The full production-style workflow is orchestrated by Azure Data Factory rather than by a single Databricks job.

---

## Execution Order

The intended ShopSphere execution order is:

```text
Bronze
   ↓
Silver
   ↓
CDF / MERGE
   ↓
Gold
   ↓
Validation
```

CDF/MERGE runs before Gold because the current-state Gold materialized views depend on:

```text
silver.current_orders
```

Azure Data Factory controls this end-to-end sequence.

---

## Bundle vs Lakeflow vs Wheel

These are separate concepts.

```text
Databricks Asset Bundle
=
deployment/configuration mechanism
```

It answers:

> Which pipelines, jobs, variables, environments, and artifacts should Databricks deploy?

```text
Lakeflow Declarative Pipeline
=
data transformation framework
```

It answers:

> How should Bronze, Silver, and Gold datasets be derived and maintained?

```text
Python Wheel
=
Python package artifact
```

It answers:

> How should standalone ShopSphere Python job code be packaged and installed?

Together:

```text
Source code
    ↓
Wheel

Resource YAML
    ↓
Asset Bundle
    ↓
Databricks resources

Lakeflow
    ↓
Data transformations
```

---

## Current Databricks Architecture

```text
Azure Event Hubs
        ↓
Spark Structured Streaming
        ↓
Bronze
        ↓
Silver Event History
        │
        ├────→ Streaming Gold
        │
        ↓
Delta CDF
        ↓
MERGE
        ↓
Current Orders
        ↓
Current-State Gold
        ↓
Validation
```

This bundle represents the active Databricks implementation of ShopSphere.