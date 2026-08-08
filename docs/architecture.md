# ShopSphere Architecture

ShopSphere is a mimic e-commerce data platform implemented in two stages:

1. A native local streaming implementation using Kafka, Spark Structured Streaming, and Delta Lake.
2. A managed Azure Databricks implementation using Lakeflow Spark Declarative Pipelines, Unity Catalog, Auto Loader, and Declarative Automation Bundles.

## Databricks Architecture

```text
Synthetic Order Generator
        |
        v
Python Wheel Task
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
Silver Orders
        |
        +-------------------+
        |                   |
        v                   v
Valid Orders        Invalid Orders
        |
        v
Gold Layer
        |
        +----------------------------+
        |              |             |
        v              v             v
Revenue by       Customer       Product
Category         Metrics        Metrics
        |
        v
Monitoring Views