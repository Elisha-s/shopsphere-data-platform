# ShopSphere Data Platform

ShopSphere is an end-to-end Azure data engineering project that simulates a production e-commerce event processing platform.

The platform generates customer and order events, publishes them to Azure Event Hubs, processes them through a Medallion architecture in Azure Databricks, performs incremental processing using Delta Lake Change Data Feed and MERGE, and orchestrates the complete workflow using Azure Data Factory.

The project focuses on production oriented data engineering concepts including streaming ingestion, data quality, deduplication, incremental processing, idempotency, orchestration, failure handling, and deployment automation.

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
│ Data quality validation       │
│ Deduplication                 │
│ Late event identification     │
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


TECHNOLOGY STACK - 
| Area                   | Technology                                  |
| ---------------------- | ------------------------------------------- |
| Programming            | Python                                      |
| Distributed Processing | PySpark                                     |
| Streaming              | Spark Structured Streaming                  |
| Event Ingestion        | Azure Event Hubs                            |
| Data Platform          | Azure Databricks                            |
| Storage Format         | Delta Lake                                  |
| Data Architecture      | Medallion Architecture                      |
| Incremental Processing | Delta Change Data Feed                      |
| Upserts                | Delta Lake MERGE                            |
| Orchestration          | Azure Data Factory                          |
| Governance             | Unity Catalog                               |
| Deployment             | Databricks Asset Bundles                    |
| Authentication         | Azure Managed Identity / Databricks Secrets |
| Version Control        | Git / GitHub                                |

### Incremental Processing with Change Data Feed
Silver Delta Table
        │
        │ Change Data Feed
        ▼
New / Updated Records
        │
        ▼
Delta MERGE
        │
        ▼
current_orders

### ADF Orchestration
Bronze Refresh
      ↓
Wait / Status Polling
      ↓
Bronze Success Gate
      ↓
Silver Refresh
      ↓
Wait / Status Polling
      ↓
Silver Success Gate
      ↓
Gold Refresh
      ↓
Wait / Status Polling
      ↓
Gold Success Gate
      ↓
CDF + MERGE
      ↓
Output Validation
