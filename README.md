# ShopSphere Data Platform

ShopSphere is an end-to-end Azure data engineering project that simulates a production e-commerce event processing platform.

The platform generates customer and order events, publishes them to Azure Event Hubs, processes them through a Medallion architecture in Azure Databricks, performs incremental processing using Delta Lake Change Data Feed and MERGE, and orchestrates the complete workflow using Azure Data Factory.

The project focuses on production-oriented data engineering concepts including streaming ingestion, data quality, deduplication, incremental processing, idempotency, orchestration, failure handling, and deployment automation.

---

## Architecture

```text
                         ┌─────────────────────────┐
                         │   Azure Data Factory    │
                         │     Orchestration       │
                         └────────────┬────────────┘
                                      │
                 Bronze → Silver → Gold → CDF/MERGE → Validation
                                      │
                                      ▼

Python Event Generators
        │
        ▼
Azure Event Hubs
        │
        │ JSON events
        ▼
Azure Databricks
        │
        ▼
┌───────────────────────────────┐
│            BRONZE             │
│ Raw Event Hubs events         │
│ Transport metadata retained   │
└──────────────┬────────────────┘
               │
               ▼
┌───────────────────────────────┐
│            SILVER             │
│ Parsing & type standardization│
│ Data-quality validation       │
│ Deduplication                 │
│ Late-event identification     │
└───────────┬───────────┬───────┘
            │           │
            ▼           ▼
      Valid Orders   Invalid Orders
            │
            ▼
┌───────────────────────────────┐
│             GOLD              │
│ Revenue by category           │
│ Product metrics               │
│ Customer metrics              │
└───────────────────────────────┘

Silver Orders
      │
      │ Delta Change Data Feed
      ▼
Incremental CDF Processor
      │
      ▼
Delta MERGE
      │
      ▼
current_orders
