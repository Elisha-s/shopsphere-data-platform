# ShopSphere Data Platform

ShopSphere is an end-to-end e-commerce data engineering platform built in two stages:

1. A native streaming implementation using Kafka, Apache Spark Structured Streaming, Docker, and Delta Lake.
2. A managed Azure Databricks implementation using Lakeflow Spark Declarative Pipelines, Unity Catalog, Auto Loader, Delta Lake, Python wheel tasks, and Declarative Automation Bundles.

The project demonstrates streaming ingestion, medallion architecture, late-arriving data handling, data-quality enforcement, business aggregations, deployment automation, and operational monitoring.

---

## Architecture

```text
Synthetic Order Generator
        |
        v
Python Wheel Job
        |
        v
Unity Catalog Volume
        |
        v
Auto Loader
        |
        v
Bronze Streaming Table
        |
        v
Silver Processing
   |            |
   v            v
Valid Orders   Invalid Orders
        |
        v
Gold Analytics
   |             |              |
   v             v              v
Revenue by   Customer       Product
Category     Metrics        Metrics
        |
        v
Lakeflow Event Log
        |
        v
Monitoring Views
